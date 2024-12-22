from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lag, unix_timestamp, from_unixtime, when, sum as _sum, min, max, count, avg, current_timestamp, concat, lit, to_date, countDistinct, year, month, rand, broadcast, desc


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

    dau_df = dau_df.select(
        col("interaction_date"),
        col("dau"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )

    # Calculate Monthly Active User: Group by year and month and count distinct users
    mau_df = df.groupBy("year", "month").agg(countDistinct("user_id").alias("mau"))

    mau_df = mau_df.select(
        col("year"),
        col("month"),
        col("mau"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )

    # Show the result
    # dau_df.show()
    # mau_df.show()

    # Show execution plan for future optimization
    # dau_df.explain("extended")
    # mau_df.explain("extended")
    
    return dau_df, mau_df

def calculate_sessions(df: DataFrame) -> DataFrame:
    """
    Calculate user sessions and session metrics.

    Args:
        df (DataFrame): Input DataFrame with user interactions.

    Returns:
        DataFrame: Session metrics DataFrame.
    """

    df_filtered = df.withColumn("action_end_time", from_unixtime(unix_timestamp("timestamp") + (col("duration_ms") / 1000).cast("bigint")))

    window_user_time = Window.partitionBy("user_id").orderBy("timestamp")

    df_with_gaps = df_filtered.withColumn("previous_action_end_time", 
                                        lag("action_end_time").over(window_user_time)) \
        .withColumn("gap", 
                    when(col("previous_action_end_time").isNotNull(), 
                        unix_timestamp("timestamp") - unix_timestamp("previous_action_end_time")) \
                    .otherwise(0)) \
                          .withColumn("is_new_session", when((col("gap") > 1200) | (col("gap") == 0), 1).otherwise(0))

    df_session_assignments = df_with_gaps.withColumn("session_id", 
        _sum("is_new_session").over(window_user_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)))

    # Calculate session durations base on duration_ms
    df_session_durations = df_session_assignments.groupBy("user_id", "session_id") \
        .agg(min("timestamp").alias("session_start"),
            max("action_end_time").alias("session_end"),
            count("action_type").alias("actions_per_session"),
            (unix_timestamp(max("action_end_time")) - unix_timestamp(min("timestamp"))).alias("session_duration_seconds"))

    # Compute session metrics with average session duration per user
    window_user = Window.partitionBy("user_id")

    df_session_metrics = df_session_durations.withColumn("avg_session_duration", 
                                                        avg("session_duration_seconds").over(window_user)) \
        .withColumn("session_id", concat(col("user_id"), lit("-"), col("session_id")))

    # Final DataFrame with rounded metrics and additional metadata
    session_df = df_session_metrics.select(
        col("user_id"),
        col("session_id"),
        col("actions_per_session"),
        col("avg_session_duration").alias("avg_session_duration_seconds"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    ).orderBy("user_id")

    # Show the result
    # session_df.show()

    # Show execution plan for future optimization
    # session_df.explain("extended")

    return session_df

def join_dataframe(df_1: DataFrame, df_2: DataFrame) -> DataFrame:
    """
    Calculate user sessions and session metrics.

    Args:
        df_1 (DataFrame): Input DataFrame with user interactions.
        df_2 (DataFrame): Input DataFrame with user metadata.

    Returns:
        DataFrame: Joined DataFrame.
    """
    user_interactions_df = df_1
    user_metadata_df = df_2
    # user_interactions_df.groupBy("user_id").count().orderBy(desc("count")).show(50)

    # Repartition the user_interaction_df by the join key to mitigate data skew
    # user_interactions_df_repartitioned = user_interactions_df.repartition(4, "user_id") 

    # Salt the user_id by adding a random number
    # user_interactions_salted = user_interactions_df.withColumn("salt", (rand() * 10).cast("int"))
    # user_metadata_salted = user_metadata_df.withColumn("salt", (rand() * 10).cast("int"))

    # # Join on salted user_id
    # joined_df = user_interactions_salted.join(broadcast(user_metadata_salted), 
    #                                         (user_interactions_salted.user_id == user_metadata_salted.user_id) & 
    #                                         (user_interactions_salted.salt == user_metadata_salted.salt), 'inner')

    # # Drop salt column after join
    # joined_df = joined_df.drop("salt")

    joined_df = user_interactions_df.join(broadcast(user_metadata_df), 
                                            (user_interactions_df.user_id == user_metadata_df.user_id), 'inner')


    joined_df = joined_df.drop(user_metadata_df["user_id"])

    # Show the result
    # joined_df.show()

    return joined_df