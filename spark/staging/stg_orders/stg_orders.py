from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_window, write_partitions


load_date = parse_load_date()
spark = make_spark()
try:
    orders = read_window(spark, "raw/orders", load_date)
    result = orders.select(
        "order_id",
        "customer_id",
        F.col("order_created_at").cast("timestamp").alias("ordered_at"),
        F.to_date("order_created_at").alias("order_date"),
        F.col("status").alias("order_status"),
        F.col("total_amount").alias("order_amount"),
        (F.col("status") == "test").alias("is_test_order"),
    )
    write_partitions(spark, result, "staging/stg_orders", load_date)
finally:
    spark.stop()
