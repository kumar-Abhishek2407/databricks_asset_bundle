from src.framework.spark_helpers import table_name, ensure_schema

from pyspark.sql.functions import avg, count, round
df = spark.read.table(table_name(ctx, "silver_schema", "jc_citibike"))
df = df.groupBy(
    "trip_start_date",
    "start_station_name"
).agg(
    round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
    count("ride_id").alias("total_trips")
)
ensure_schema(spark, ctx, "gold_schema")

df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name(ctx, "gold_schema", "daily_station_performance"))
