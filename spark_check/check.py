import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import functions as F

from common import (
    PARTITIONS, RAW_COLUMNS, make_spark, parquet_files, read_jdbc,
    read_table, read_window, table_path,
)


DATE_COLUMNS = {
    "orders": ("order_created_at", "order_created_dt"),
    "payments": ("paid_at", "paid_dt"),
    "events": ("event_time", "event_dt"),
}
KEYS = {
    "customers": ["customer_id"], "orders": ["order_id"],
    "payments": ["payment_id"], "events": ["event_id"],
    "products": ["product_id"], "product_categories": ["category_id"],
    "order_items": ["order_id", "product_id"],
}
CHECK_NAMES = [f"raw_{name}" for name in RAW_COLUMNS] + [
    "stg_orders", "stg_payments", "int_orders_with_payments", "fct_orders",
]


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")


def outside_files(spark, table, load_date):
    column = PARTITIONS[table]
    files = parquet_files(spark, table_path(table))
    start = load_date - timedelta(days=3)
    outside = {}
    for path, metadata in files.items():
        label = path.split(f"/{column}=", 1)[1].split("/", 1)[0]
        inside = label == "__HIVE_DEFAULT_PARTITION__" and column == "paid_dt"
        if label != "__HIVE_DEFAULT_PARTITION__":
            inside = start <= date.fromisoformat(label) < load_date
        if not inside:
            outside[path] = metadata
    return {"initialized": bool(files), "files": outside}


def fingerprint(df):
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.xxhash64(*sorted(df.columns)).cast("decimal(38,0)")).alias("checksum"),
    ).first()
    return [row["rows"], str(row["checksum"])]


def assert_unique(df, keys):
    assert not df.filter(" OR ".join(f"{key} IS NULL" for key in keys)).take(1), f"NULL key: {keys}"
    assert not df.groupBy(*keys).count().filter(F.col("count") > 1).take(1), f"Duplicate key: {keys}"


def assert_same(actual, expected):
    assert set(actual.columns) == set(expected.columns), f"Columns differ: {actual.columns} / {expected.columns}"
    columns = expected.columns
    actual = actual.select(*columns)
    assert not actual.exceptAll(expected).take(1), "Extra or changed rows in sink"
    assert not expected.exceptAll(actual).take(1), "Missing or changed rows in sink"
    return actual.count()


def raw_check(spark, table, load_date, source_cache):
    where = ""
    if table in DATE_COLUMNS:
        timestamp, partition = DATE_COLUMNS[table]
        start = load_date - timedelta(days=3)
        pending = " OR paid_at IS NULL" if table == "payments" else ""
        where = f"WHERE (({timestamp} >= '{start}'::timestamp AND {timestamp} < '{load_date}'::timestamp){pending})"
        actual = read_window(spark, f"raw/{table}", load_date)
    else:
        actual = read_table(spark, f"raw/{table}")
    expected = read_jdbc(spark, table, where).cache()
    source_cache[table] = expected
    if table in DATE_COLUMNS:
        timestamp, partition = DATE_COLUMNS[table]
        assert dict(actual.dtypes)[partition] == "date", f"Invalid partition type: {partition}"
        expected = expected.withColumn(partition, F.to_date(timestamp))
    count = assert_same(actual, expected)
    assert_unique(actual, KEYS[table])
    if table == "events":
        assert not read_table(spark, "raw/events").filter(F.col("event_dt").isNull()).take(1), "NULL event_dt"
    return f"{count} rows match PostgreSQL"


def staging_orders_check(spark, load_date):
    actual = read_window(spark, "staging/stg_orders", load_date)
    expected = read_window(spark, "raw/orders", load_date).selectExpr(
        "order_id", "customer_id", "CAST(order_created_at AS TIMESTAMP) AS ordered_at",
        "CAST(order_created_at AS DATE) AS order_date", "status AS order_status",
        "total_amount AS order_amount", "status = 'test' AS is_test_order",
    )
    assert len(actual.columns) == 7
    types = dict(actual.dtypes)
    assert types["ordered_at"] == "timestamp" and types["order_date"] == "date"
    assert types["is_test_order"] == "boolean"
    count = assert_same(actual, expected)
    assert_unique(actual, ["order_id", "order_date"])
    return f"{count} rows, 7 columns, types and values match raw"


def staging_payments_check(spark, load_date):
    actual = read_window(spark, "staging/stg_payments", load_date)
    expected = read_window(spark, "raw/payments", load_date).selectExpr(
        "payment_id", "order_id", "CAST(paid_at AS TIMESTAMP) AS paid_at", "payment_status", "amount",
        "payment_status = 'success' AND amount < 0 AS is_refund", "CAST(paid_at AS DATE) AS paid_dt",
    )
    assert len(actual.columns) == 7
    types = dict(actual.dtypes)
    assert types["paid_at"] == "timestamp" and types["paid_dt"] == "date" and types["is_refund"] == "boolean"
    count = assert_same(actual, expected)
    assert_unique(actual, ["payment_id"])
    return f"{count} rows including pending, 7 columns, refund flags match raw"


def intermediate_check(spark, load_date):
    read_window(spark, "staging/stg_orders", load_date).createOrReplaceTempView("orders_window")
    read_window(spark, "staging/stg_payments", load_date).createOrReplaceTempView("payments_window")
    expected = spark.sql("""
        WITH totals AS (
            SELECT order_id,
                COUNT(*) FILTER (WHERE payment_status = 'success' AND NOT is_refund) AS successful_payments_count,
                COALESCE(SUM(amount) FILTER (WHERE payment_status = 'success' AND NOT is_refund), 0) AS paid_amount,
                COALESCE(-SUM(amount) FILTER (WHERE is_refund), 0) AS refunded_amount,
                MIN(paid_at) FILTER (WHERE payment_status = 'success' AND NOT is_refund) AS first_paid_at,
                COUNT(*) FILTER (WHERE payment_status = 'success') AS success_operations_count,
                COUNT(*) FILTER (WHERE payment_status = 'failed') AS failed_operations_count,
                COUNT(*) FILTER (WHERE payment_status = 'pending') AS pending_operations_count
            FROM payments_window GROUP BY order_id
        )
        SELECT o.order_id, o.customer_id, o.ordered_at, o.order_date, o.order_status,
            o.order_amount, o.is_test_order,
            COALESCE(p.successful_payments_count, 0) AS successful_payments_count,
            COALESCE(p.paid_amount, 0) AS paid_amount,
            COALESCE(p.refunded_amount, 0) AS refunded_amount,
            COALESCE(p.paid_amount, 0) - COALESCE(p.refunded_amount, 0) AS net_paid_amount,
            p.first_paid_at,
            o.order_status = 'paid' AND COALESCE(p.successful_payments_count, 0) > 0 AS is_confirmed,
            COALESCE(p.success_operations_count, 0) AS success_operations_count,
            COALESCE(p.failed_operations_count, 0) AS failed_operations_count,
            COALESCE(p.pending_operations_count, 0) AS pending_operations_count
        FROM orders_window o LEFT JOIN totals p ON o.order_id = p.order_id
    """)
    actual = read_window(spark, "intermediate/int_orders_with_payments", load_date)
    assert len(actual.columns) == 16
    assert dict(actual.dtypes)["is_confirmed"] == "boolean"
    count = assert_same(actual, expected)
    assert_unique(actual, ["order_id", "order_date"])
    return f"{count} rows, 16 columns, aggregates and LEFT JOIN match reference SQL"


def marts_check(spark, load_date):
    actual = read_window(spark, "marts/fct_orders", load_date)
    expected = read_window(spark, "intermediate/int_orders_with_payments", load_date).filter(
        ~F.col("is_test_order")
    ).select(
        "order_id", "customer_id", "order_date", "ordered_at", "order_status", "order_amount",
        "paid_amount", "refunded_amount", "net_paid_amount", "successful_payments_count",
        "first_paid_at", "is_confirmed",
    )
    assert len(actual.columns) == 12
    assert not actual.filter(F.col("order_status") == "test").take(1), "Test order in marts"
    count = assert_same(actual, expected)
    assert_unique(actual, ["order_id", "order_date"])
    return f"{count} rows, 12 columns, test orders excluded"


def append_log(args, check, status, message):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "run_id": args.run_id, "load_date": args.load_date.isoformat(),
        "check": check, "status": status, "message": message,
    }
    if args.log_file:
        path = Path(args.log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(f"[{status}] {check}: {message}", flush=True)


def run_checks(spark, args):
    sources = {}
    failures = 0
    names = CHECK_NAMES if args.check == "all" else [args.check]
    try:
        for name in names:
            try:
                if name.startswith("raw_"):
                    message = raw_check(spark, name[4:], args.load_date, sources)
                else:
                    fn = {
                        "stg_orders": staging_orders_check,
                        "stg_payments": staging_payments_check,
                        "int_orders_with_payments": intermediate_check,
                        "fct_orders": marts_check,
                    }[name]
                    message = fn(spark, args.load_date)
                append_log(args, name, "PASSED", message)
            except Exception as error:
                failures += 1
                append_log(args, name, "FAILED", f"{type(error).__name__}: {error}")
        if args.check == "all" and failures == 0:
            before = json.loads(args.state_file.read_text())
            for table in PARTITIONS:
                old = before[table]
                current = outside_files(spark, table, args.load_date)
                first_raw_load = table.startswith("raw/") and not old["initialized"]
                assert first_raw_load or old["files"] == current["files"], f"Outside partitions changed: {table}"
            append_log(args, "outside_window", "PASSED", "Files outside the window are unchanged")
            inputs = {name: fingerprint(df) for name, df in sources.items()}
            outputs = {}
            for table in PARTITIONS:
                outputs[table] = fingerprint(read_window(spark, table, args.load_date))
            for name in ("customers", "products", "product_categories", "order_items"):
                outputs[f"raw/{name}"] = fingerprint(read_table(spark, f"raw/{name}"))
            history_path = args.state_file.parent / f"last-{args.load_date}.json"
            if history_path.exists():
                previous = json.loads(history_path.read_text())
                if previous["inputs"] == inputs:
                    assert previous["outputs"] == outputs, "Repeat run changed output for unchanged input"
                    message = "Same date and input produce identical output"
                else:
                    message = "Source changed; output checked against current source"
            else:
                message = "First run for this date; fingerprint saved for repeat check"
            write_json(history_path, {"inputs": inputs, "outputs": outputs})
            append_log(args, "idempotence", "PASSED", message)
    except Exception as error:
        failures += 1
        append_log(args, "pipeline_invariants", "FAILED", f"{type(error).__name__}: {error}")
    finally:
        for df in sources.values():
            df.unpersist()
    return failures


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", choices=["all", "snapshot"] + CHECK_NAMES, default="all")
    parser.add_argument("--load-date", required=True, type=date.fromisoformat)
    parser.add_argument("--log-file", default="checks.log")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-file", type=Path, default=Path(__file__).parent / ".state/before.json")
    args = parser.parse_args()
    spark = make_spark()
    try:
        if args.check == "snapshot":
            write_json(args.state_file, {
                table: outside_files(spark, table, args.load_date) for table in PARTITIONS
            })
            return 0
        return 1 if run_checks(spark, args) else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
