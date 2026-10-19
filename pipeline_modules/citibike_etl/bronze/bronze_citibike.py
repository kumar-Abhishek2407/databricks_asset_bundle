def run(ctx):
    spark = ctx.spark
    config = ctx.runtime_config
    catalog = config["catalog"]

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"/Volumes/{catalog}/{config['landing_schema']}/{config['source_volume']}/{config['source_file']}")
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{config['bronze_schema']}`")

    df.write.\
    mode("overwrite").\
    option("overwriteSchema", "true").\
    saveAsTable(f"`{catalog}`.`{config['bronze_schema']}`.`jc_citibike`")
