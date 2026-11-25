from common import make_spark, read_jdbc, table_path


spark = make_spark()
try:
    # В источнике нет даты изменения или создания позиции, поэтому загрузка полная.
    df = read_jdbc(spark, "order_items")
    df.write.mode("overwrite").parquet(table_path("raw/order_items"))
    print("Saved raw/order_items")
finally:
    spark.stop()
