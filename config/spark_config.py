from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "Spark Application") -> SparkSession:
    """
    Create a Spark session.

    Args:
        app_name (str): Application name for the Spark session.

    Returns:
        SparkSession: Spark session instance.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
