from src.framework.spark_helpers import table_name, ensure_schema, volume_file_path

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(volume_file_path(ctx))
)

ensure_schema(spark, ctx, "bronze_schema")

df.write.\
mode("overwrite").\
option("overwriteSchema", "true").\
saveAsTable(table_name(ctx, "bronze_schema", "jc_citibike"))
