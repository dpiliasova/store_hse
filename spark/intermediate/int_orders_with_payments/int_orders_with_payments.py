from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_window, write_partitions


load_date = parse_load_date()
spark = make_spark()
try:
    orders = read_window(spark, "staging/stg_orders", load_date)
    payments = read_window(spark, "staging/stg_payments", load_date)
    successful = (F.col("payment_status") == "success") & ~F.col("is_refund")
    totals = payments.groupBy("order_id").agg(
        F.count(F.when(successful, 1)).alias("successful_payments_count"),
        F.sum(F.when(successful, F.col("amount"))).alias("paid_amount"),
        (-F.sum(F.when(F.col("is_refund"), F.col("amount")))).alias("refunded_amount"),
        F.min(F.when(successful, F.col("paid_at"))).alias("first_paid_at"),
        *[
            F.count(F.when(F.col("payment_status") == status, 1)).alias(f"{status}_operations_count")
            for status in ("success", "failed", "pending")
        ],
    )
    result = (
        orders.join(totals, on="order_id", how="left")
        .fillna(0, subset=[
            "successful_payments_count", "paid_amount", "refunded_amount",
            "success_operations_count", "failed_operations_count", "pending_operations_count",
        ])
        .withColumn("net_paid_amount", F.col("paid_amount") - F.col("refunded_amount"))
        .withColumn("is_confirmed", (F.col("order_status") == "paid") & (F.col("successful_payments_count") > 0))
        .select(
            "order_id", "customer_id", "ordered_at", "order_date", "order_status",
            "order_amount", "is_test_order", "successful_payments_count", "paid_amount",
            "refunded_amount", "net_paid_amount", "first_paid_at", "is_confirmed",
            "success_operations_count", "failed_operations_count", "pending_operations_count",
        )
    )
    write_partitions(spark, result, "intermediate/int_orders_with_payments", load_date)
finally:
    spark.stop()
