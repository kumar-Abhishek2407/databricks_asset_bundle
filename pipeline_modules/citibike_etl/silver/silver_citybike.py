def run(ctx):
    spark = ctx.spark
    config = ctx.runtime_config
    catalog = config["catalog"]

    from src.citibike.citibike_utils import get_trip_duration_mins
    from src.utils.datetime_utils import timestamp_to_date_col
    df = spark.read.table(f"`{catalog}`.`{config['bronze_schema']}`.`jc_citibike`")
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
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{config['silver_schema']}`")

    df.write.\
    mode("overwrite").\
    option("overwriteSchema", "true").\
    saveAsTable(f"`{catalog}`.`{config['silver_schema']}`.`jc_citibike`")
