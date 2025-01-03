import logging
from config.spark_config import get_spark_session
from src.analytic_functions import calculate_sessions, calculate_dau_and_mau, join_dataframe, to_date
from src.utils import read_csv, write_parquet, read_parquet, validate_df_columns, validate_null_values, validate_unique_values

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize Spark session
        spark = get_spark_session("ProductionPipeline")

        # Load input data to spark to read the data
        user_interactions_file_path = "data/parquet/user_interactions_sample.parquet"  
        user_metadata_file_path = "data/parquet/user_metadata_sample.parquet"

        user_interactions_df = read_parquet(spark, user_interactions_file_path)
        user_metadata_df = read_parquet(spark, user_metadata_file_path)

        user_interactions_schema = ["user_id", "timestamp", "action_type", "page_id", "duration_ms"]
        user_metadata_schema = ['user_id', 'join_date', 'country', 'device_type', 'subscription_type']

        # Data quality checks
        # Columns schema match
        validate_df_columns(user_interactions_df, user_interactions_schema)
        validate_df_columns(user_metadata_df, user_metadata_schema)

        # Null values
        validate_null_values(user_interactions_df, user_interactions_schema)
        validate_null_values(user_metadata_df, user_metadata_schema)

        # Unique values
        validate_unique_values(user_metadata_df, ["user_id"])

        # Perform DAU/MAU calculations
        logger.info("Starting DAU/MAU calculations.")
        dau_df, mau_df = calculate_dau_and_mau(user_interactions_df.select("user_id", "timestamp"))

        # Write DAU/MAU analysis data to parquet file
        write_parquet(dau_df, "output/daily_active_user_analysis.parquet")
        write_parquet(mau_df, "output/monthly_active_user_analysis.parquet")

        # Perform session analysis
        logger.info("Starting session analysis.")
        session_df = calculate_sessions(user_interactions_df)

        # Write Session analysis data to parquet file
        write_parquet(session_df, "output/session_analysis.parquet")

        # Perform join user interactions and user metadata
        logger.info("Starting joining datasets")
        joined_df = join_dataframe(user_interactions_df, user_metadata_df)

        # Write Session analysis data to parquet file
        write_parquet(joined_df, "output/joined_datasets.parquet")
        logger.info("All tasks completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred in the main workflow: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down Spark session.")
        spark.stop()

        # Keep the spark session open to access Spark UI
        # input()

if __name__ == "__main__":
    main()