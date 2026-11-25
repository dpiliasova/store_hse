from datetime import timedelta

from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_jdbc, sink_has_data, write_partitions


load_date = parse_load_date()
start_date = load_date - timedelta(days=3)
spark = make_spark()
try:
    full_load = not sink_has_data(spark, "raw/events")
    where = "" if full_load else (
        f"WHERE ((event_time >= '{start_date}'::timestamp "
        f"AND event_time < '{load_date}'::timestamp))"
    )

    df = read_jdbc(spark, "events", where).withColumn("event_dt", F.to_date("event_time"))
    # Dynamic overwrite сохраняет партиции вне окна. Пустые партиции окна очищаются в write_partitions.
    write_partitions(spark, df, "raw/events", load_date, full_load=full_load)
finally:
    spark.stop()
