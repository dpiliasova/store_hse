from common import make_spark, read_jdbc, table_path


spark = make_spark()
try:
    df = read_jdbc(spark, "products")
    df.write.mode("overwrite").parquet(table_path("raw/products"))
    print("Saved raw/products")
finally:
    spark.stop()
