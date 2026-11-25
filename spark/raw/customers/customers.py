from common import make_spark, read_jdbc, table_path


spark = make_spark()
try:
    # Справочник перечитывается целиком без окна и partitionBy.
    # Окно по регистрации пропустит изменения города у старых клиентов.
    # sink_has_data не нужен: полный overwrite заменяет предыдущий снимок.
    df = read_jdbc(spark, "customers")
    df.write.mode("overwrite").parquet(table_path("raw/customers"))
    print("Saved raw/customers")
finally:
    spark.stop()
