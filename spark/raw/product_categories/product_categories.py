from common import make_spark, read_jdbc, table_path


spark = make_spark()
try:
    df = read_jdbc(spark, "product_categories")
    df.write.mode("overwrite").parquet(table_path("raw/product_categories"))
    print("Saved raw/product_categories")
finally:
    spark.stop()
