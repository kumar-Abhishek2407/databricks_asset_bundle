from pyspark.sql.functions import to_date, col


def timestamp_to_date_col(spark, df, timestamp_col, output_col):
    """
    Extracts the date from a timestamp column and adds it as a new column.

    Parameters:
        spark: SparkSession.
        df (DataFrame): Input PySpark DataFrame.
        timestamp_col (str): Name of the timestamp column.
        output_col (str): Name of the output date column.

    Returns:
        DataFrame: DataFrame with the additional date column.
    """
    return df.withColumn(output_col, to_date(col(timestamp_col)))