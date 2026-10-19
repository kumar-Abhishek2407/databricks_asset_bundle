def run(ctx):
    spark = ctx.spark
    config = ctx.runtime_config
    catalog = config["catalog"]

    from pyspark.sql.functions import max, min, avg, count, round
    df = spark.read.table(f"`{catalog}`.`{config['silver_schema']}`.`jc_citibike`")
    df = df.groupBy("trip_start_date").agg(
        round(max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
        round(min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
        round(avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        count("ride_id").alias("total_trips")
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{config['gold_schema']}`")

    df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"`{catalog}`.`{config['gold_schema']}`.`daily_ride_summary`")
