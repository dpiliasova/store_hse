from datetime import timedelta

from pyspark.sql import functions as F

from common import make_spark, parse_load_date, read_jdbc, sink_has_data, write_partitions


load_date = parse_load_date()
start_date = load_date - timedelta(days=3)
spark = make_spark()
try:
    full_load = not sink_has_data(spark, "raw/payments")
    where = "" if full_load else (
        f"WHERE ((paid_at >= '{start_date}'::timestamp "
        f"AND paid_at < '{load_date}'::timestamp) OR paid_at IS NULL)"
    )
    # paid_at IS NULL попадет в paid_dt=__HIVE_DEFAULT_PARTITION__.
    df = read_jdbc(spark, "payments", where).withColumn("paid_dt", F.to_date("paid_at"))
    # Dynamic overwrite сохраняет партиции вне окна. Пустые партиции окна очищаются в write_partitions.
    write_partitions(spark, df, "raw/payments", load_date, full_load=full_load)
finally:
    spark.stop()
