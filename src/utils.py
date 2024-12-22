import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when


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

def read_parquet(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read a Parquet file into a DataFrame.

    Args:
        spark (SparkSession): Spark session.
        file_path (str): Path to the Parquet file.

    Returns:
        DataFrame: Loaded DataFrame.
    """
    try:
        logger.info(f"Attempting to read Parquet file from: {file_path}")
        df = spark.read.parquet(file_path, header=True, inferSchema=True)
        logger.info(f"Successfully read CSV Parquet with {df.count()} rows and {len(df.columns)} columns.")
        return df
    except Exception as e:
        logger.error(f"Error while reading CSV Parquet from {file_path}: {str(e)}", exc_info=True)
        raise

def validate_df_columns(df: DataFrame, required_columns: list) -> None:
    """
    Validate if required columns exist in the DataFrame.
    """
    try:
        logger.info("Validating DataFrame schema.")
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_message = f"DataFrame is missing required columns: {missing_columns}"
            logger.error(error_message)
            raise ValueError(error_message)
        logger.info("DataFrame schema validation successful.")
    except Exception as e:
        logger.error("Error during DataFrame validation.", exc_info=True)
        raise

def validate_null_values(df, columns):
    """
    Validate if the specified columns in the DataFrame have null values.

    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to validate for null values.

    Returns:
        dict: A dictionary where keys are column names and values are True (no nulls) or False (contains nulls).
    """
    try:
        logger.info("Validating null values in DataFrame columns.")
        missing_columns = [column for column in columns if column not in df.columns]
        if missing_columns:
            error_message = f"DataFrame is missing required columns: {missing_columns}"
            logger.error(error_message)
            raise ValueError(error_message)

        result = {}
        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            result[column] = (null_count == 0)
            logger.info(f"Column '{column}' validation: {'No nulls' if result[column] else 'Contains nulls'}.")

        logger.info("Null value validation completed successfully.")
        return result
    except Exception as e:
        logger.error("Error during null value validation.", exc_info=True)
        raise

def validate_unique_values(df, columns):
    """
    Validate if the specified columns in the DataFrame have unique values.

    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to validate for uniqueness.

    Returns:
        dict: A dictionary where keys are column names and values are True (unique) or False (not unique).
    """
    try:
        logger.info("Validating unique values in DataFrame columns.")
        missing_columns = [column for column in columns if column not in df.columns]
        if missing_columns:
            error_message = f"DataFrame is missing required columns: {missing_columns}"
            logger.error(error_message)
            raise ValueError(error_message)

        result = {}
        for column in columns:
            total_count = df.count()
            unique_count = df.select(column).distinct().count()
            result[column] = (total_count == unique_count)
            logger.info(f"Column '{column}' validation: {'Unique' if result[column] else 'Not unique'}.")

        logger.info("Unique value validation completed successfully.")
        return result
    except Exception as e:
        logger.error("Error during unique value validation.", exc_info=True)
        raise
