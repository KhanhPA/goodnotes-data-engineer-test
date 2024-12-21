import logging
from config.spark_config import get_spark_session
from src.analytic_functions import calculate_sessions, calculate_dau_and_mau
from src.utils import read_csv, write_parquet

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize Spark session
        spark = get_spark_session("ProductionPipeline")

        # Load input data to spark to read the data
        file_path = "data/csv/user_interactions_sample.csv"  
        df = read_csv(spark, file_path)

        # Perform DAU/MAU calculations
        logger.info("Starting DAU/MAU calculations.")
        dau_df, mau_df = calculate_dau_and_mau(df)

        # Write DAU/MAU analysis data to parquet file
        write_parquet(dau_df, "output/daily_active_user_analysis.parquet")
        write_parquet(mau_df, "output/monthly_active_user_analysis.parquet")

        # Perform session analysis
        logger.info("Starting session analysis.")
        session_df = calculate_sessions(df)

        # Write Session analysis data to parquet file
        write_parquet(session_df, "output/session_analysis.parquet")
        logger.info("All tasks completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred in the main workflow: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down Spark session.")
        # spark.stop()
        input()

if __name__ == "__main__":
    main()