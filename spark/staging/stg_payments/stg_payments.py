from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_window, write_partitions


load_date = parse_load_date()
spark = make_spark()
try:
    payments = read_window(spark, "raw/payments", load_date)
    result = payments.select(
        "payment_id",
        "order_id",
        F.col("paid_at").cast("timestamp").alias("paid_at"),
        "payment_status",
        "amount",
        ((F.col("payment_status") == "success") & (F.col("amount") < 0)).alias("is_refund"),
        F.to_date("paid_at").alias("paid_dt"),
    )
    write_partitions(spark, result, "staging/stg_payments", load_date)
finally:
    spark.stop()
