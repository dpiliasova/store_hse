from datetime import timedelta

from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_jdbc, sink_has_data, write_partitions


load_date = parse_load_date()
start_date = load_date - timedelta(days=3)
spark = make_spark()
try:
    full_load = not sink_has_data(spark, "raw/orders")
    where = "" if full_load else (
        f"WHERE ((order_created_at >= '{start_date}'::timestamp "
        f"AND order_created_at < '{load_date}'::timestamp))"
    )

    df = read_jdbc(spark, "orders", where).withColumn("order_created_dt", F.to_date("order_created_at"))
    # Dynamic overwrite сохраняет партиции вне окна. Пустые партиции окна очищаются в write_partitions.
    write_partitions(spark, df, "raw/orders", load_date, full_load=full_load)
finally:
    spark.stop()
