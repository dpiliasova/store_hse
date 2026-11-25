import argparse
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F


BUCKET = "de-course"
RAW_COLUMNS = {
    "customers": "customer_id, registered_at, city, acquisition_channel",
    "orders": "order_id, customer_id, order_created_at, status, total_amount",
    "payments": "payment_id, order_id, paid_at, payment_status, amount",
    "events": "event_id, customer_id, event_time, event_type, product_id, session_id",
    "products": "product_id, category, brand, product_name",
    "product_categories": "category_id, parent_category_id, category_name",
    "order_items": "order_id, product_id, quantity, unit_price, discount_amount",
}
SCHEMAS = {
    "raw/customers": "customer_id int, registered_at timestamp, city string, acquisition_channel string",
    "raw/orders": "order_id int, customer_id int, order_created_at timestamp, status string, total_amount decimal(12,2), order_created_dt date",
    "raw/payments": "payment_id int, order_id int, paid_at timestamp, payment_status string, amount decimal(12,2), paid_dt date",
    "raw/events": "event_id int, customer_id int, event_time timestamp, event_type string, product_id int, session_id string, event_dt date",
    "raw/products": "product_id int, category string, brand string, product_name string",
    "raw/product_categories": "category_id int, parent_category_id int, category_name string",
    "raw/order_items": "order_id int, product_id int, quantity int, unit_price decimal(12,2), discount_amount decimal(12,2)",
    "staging/stg_orders": "order_id int, customer_id int, ordered_at timestamp, order_date date, order_status string, order_amount decimal(12,2), is_test_order boolean",
    "staging/stg_payments": "payment_id int, order_id int, paid_at timestamp, payment_status string, amount decimal(12,2), is_refund boolean, paid_dt date",
    "intermediate/int_orders_with_payments": "order_id int, customer_id int, ordered_at timestamp, order_date date, order_status string, order_amount decimal(12,2), is_test_order boolean, successful_payments_count bigint, paid_amount decimal(22,2), refunded_amount decimal(22,2), net_paid_amount decimal(23,2), first_paid_at timestamp, is_confirmed boolean, success_operations_count bigint, failed_operations_count bigint, pending_operations_count bigint",
    "marts/fct_orders": "order_id int, customer_id int, order_date date, ordered_at timestamp, order_status string, order_amount decimal(12,2), paid_amount decimal(22,2), refunded_amount decimal(22,2), net_paid_amount decimal(23,2), successful_payments_count bigint, first_paid_at timestamp, is_confirmed boolean",
}
PARTITIONS = {
    "raw/orders": "order_created_dt",
    "raw/payments": "paid_dt",
    "raw/events": "event_dt",
    "staging/stg_orders": "order_date",
    "staging/stg_payments": "paid_dt",
    "intermediate/int_orders_with_payments": "order_date",
    "marts/fct_orders": "order_date",
}


def table_path(table):
    return f"s3a://{BUCKET}/{table}"


def parse_load_date():
    parser = argparse.ArgumentParser()
    parser.add_argument("--load-date", required=True, type=date.fromisoformat)
    return parser.parse_args().load_date


def make_spark():
    spark = (
        SparkSession.builder
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    jvm = spark._jvm
    credentials = jvm.com.amazonaws.auth.BasicAWSCredentials(
        os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    )
    endpoint = jvm.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
        os.environ.get("MINIO_ENDPOINT", "http://minio:9000"), "us-east-1"
    )
    client = (
        jvm.com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
        .withEndpointConfiguration(endpoint)
        .withCredentials(jvm.com.amazonaws.auth.AWSStaticCredentialsProvider(credentials))
        .withPathStyleAccessEnabled(True)
        .build()
    )
    try:
        if not client.doesBucketExistV2(BUCKET):
            client.createBucket(BUCKET)
    finally:
        client.shutdown()
    return spark


def filesystem(spark, path):
    item = spark._jvm.org.apache.hadoop.fs.Path(path)
    return item.getFileSystem(spark._jsc.hadoopConfiguration()), item


def parquet_files(spark, path):
    fs, item = filesystem(spark, path)
    if not fs.exists(item):
        return {}
    result = {}
    iterator = fs.listFiles(item, True)
    while iterator.hasNext():
        status = iterator.next()
        name = status.getPath().toString()
        if name.endswith(".parquet"):
            result[name] = [status.getLen(), status.getModificationTime()]
    return result


def read_table(spark, table):
    path = table_path(table)
    if not parquet_files(spark, path):
        return spark.createDataFrame([], SCHEMAS[table])
    df = spark.read.parquet(path)
    if table in PARTITIONS:
        column = PARTITIONS[table]
        if dict(df.dtypes)[column] != "date":
            df = df.withColumn(column, F.col(column).cast("date"))
    return df


def sink_has_data(spark, table):
    return bool(read_table(spark, table).take(1))


def window_filter(column, load_date, include_pending=False):
    start_date = load_date - timedelta(days=3)
    condition = (F.col(column) >= F.lit(start_date)) & (F.col(column) < F.lit(load_date))
    return condition | F.col(column).isNull() if include_pending else condition


def read_window(spark, table, load_date):
    column = PARTITIONS[table]
    return read_table(spark, table).filter(
        window_filter(column, load_date, include_pending=column == "paid_dt")
    )


def read_jdbc(spark, table, where=""):
    query = f"SELECT {RAW_COLUMNS[table]} FROM public.{table} {where}"
    return (
        spark.read.format("jdbc")
        .option("query", query)
        .option("url", f"jdbc:postgresql://{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/{os.environ.get('POSTGRES_DB', 'shop')}")
        .option("user", os.environ.get("POSTGRES_USER", "student"))
        .option("password", os.environ.get("POSTGRES_PASSWORD", "studentpass"))
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_partitions(spark, df, table, load_date, full_load=False):
    path = table_path(table)
    column = PARTITIONS[table]
    df = df.persist()
    try:
        present = {row[0] for row in df.select(column).distinct().collect()}
        if present:
            df.write.mode("overwrite").partitionBy(column).parquet(path)
        if not full_load:
            dates = {load_date - timedelta(days=n) for n in (1, 2, 3)}
            if column == "paid_dt":
                dates.add(None)
            fs, _ = filesystem(spark, path)
            for value in dates - present:
                label = "__HIVE_DEFAULT_PARTITION__" if value is None else value.isoformat()
                target = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/{column}={label}")
                if fs.exists(target):
                    fs.delete(target, True)
        spark.catalog.refreshByPath(path)
    finally:
        df.unpersist()
    print(f"Saved {table}")
