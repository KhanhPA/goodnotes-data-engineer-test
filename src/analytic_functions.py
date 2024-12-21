from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, to_date, countDistinct, year, month, concat_ws


def calculate_dau_and_mau(df: DataFrame) -> DataFrame:
    """
    Calculate Daily Active Users (DAU) and Monthly Active Users (MAU).

    Args:
        df (DataFrame): Input DataFrame with user interactions.

    Returns:
        DataFrame: DAU and MAU metrics DataFrame.
    """

    # Add interaction_date and yearmonth columns
    df = df.withColumn("interaction_date", to_date(col("timestamp"))) \
        .withColumn("year", year(col("interaction_date"))) \
        .withColumn("month", month(col("interaction_date")))

    # Calculate Daily Active User: Group by interaction_date and count distinct users
    dau_df = df.groupBy("interaction_date").agg(countDistinct("user_id").alias("dau"))

    # Calculate Monthly Active User: Group by year and month and count distinct users
    mau_df = df.groupBy("year", "month").agg(countDistinct("user_id").alias("mau"))

    # Show the result
    dau_df.show()
    mau_df.show()

    # Show execution plan for future optimization
    dau_df.explain("extended")
    mau_df.explain("extended")
    
    return dau_df, mau_df
