from src.framework.spark_helpers import table_name, ensure_schema

from src.citibike.citibike_utils import get_trip_duration_mins
from src.utils.datetime_utils import timestamp_to_date_col
df = spark.read.table(table_name(ctx, "bronze_schema", "jc_citibike"))
from pyspark.sql.functions import col, to_timestamp

df = (
    df
    .withColumn(
        "started_at",
        to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss.SSS")
    )
    .withColumn(
        "ended_at",
        to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss.SSS")
    )
)
df = get_trip_duration_mins(spark, df, "started_at", "ended_at", "trip_duration_mins")
df = timestamp_to_date_col(spark, df, "started_at", "trip_start_date")

df =  df.select(
"ride_id",
"trip_start_date",
"started_at",
"ended_at",
"start_station_name",
"end_station_name", "trip_duration_mins" )
ensure_schema(spark, ctx, "silver_schema")

df.write.\
mode("overwrite").\
option("overwriteSchema", "true").\
saveAsTable(table_name(ctx, "silver_schema", "jc_citibike"))
