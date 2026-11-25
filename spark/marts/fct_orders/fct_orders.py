from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_window, write_partitions


load_date = parse_load_date()
spark = make_spark()
try:
    orders = read_window(spark, "intermediate/int_orders_with_payments", load_date)
    result = orders.filter(~F.col("is_test_order")).select(
        "order_id", "customer_id", "order_date", "ordered_at", "order_status",
        "order_amount", "paid_amount", "refunded_amount", "net_paid_amount",
        "successful_payments_count", "first_paid_at", "is_confirmed",
    )
    write_partitions(spark, result, "marts/fct_orders", load_date)
finally:
    spark.stop()
