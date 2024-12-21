import logging
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read a CSV file into a DataFrame.

    Args:
        spark (SparkSession): Spark session.
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: Loaded DataFrame.
    """
    try:
        logger.info(f"Attempting to read CSV file from: {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        logger.info(f"Successfully read CSV file with {df.count()} rows and {len(df.columns)} columns.")
        return df
    except Exception as e:
        logger.error(f"Error while reading CSV file from {file_path}: {str(e)}", exc_info=True)
        raise


def write_parquet(df: DataFrame, output_path: str) -> None:
    """
    Write a DataFrame to Parquet format.

    Args:
        df (DataFrame): DataFrame to save.
        output_path (str): Path to save the Parquet file.
    """
    try:
        logger.info(f"Attempting to write DataFrame to Parquet at: {output_path}")
        df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Successfully wrote DataFrame to Parquet at: {output_path}")
    except Exception as e:
        logger.error(f"Error while writing DataFrame to Parquet at {output_path}: {str(e)}", exc_info=True)
        raise
