import unittest
from pyspark.sql.functions import col
from datetime import datetime
from pyspark.sql import Row
from config.spark_config import get_spark_session
from src.analytic_functions import calculate_sessions, calculate_dau_and_mau, join_dataframe


# Initialize a Spark session
spark = get_spark_session("Unittestpipeline")

class TestDataProcessingFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Setup function to create the test DataFrame
        cls.test_data = [
            (1, "2024-12-01 10:00:00", 300000, "create"),
            (1, "2024-12-01 10:18:00", 200000, "share"),
            (1, "2024-12-02 09:00:00", 300000, "page_view"),
            (3, "2024-12-02 10:00:00", 200000, "share"),
            (2, "2024-12-03 14:00:00", 100000, "page_view"),
            (3, "2024-12-03 10:00:00", 200000, "delete"),
            (1, "2024-12-03 15:00:00", 100000, "page_view")
        ]
        cls.columns = ["user_id", "timestamp", "duration_ms", "action_type"]
        cls.df = spark.createDataFrame(cls.test_data, cls.columns)

    def test_calculate_dau_and_mau(self):
        # Expected output for Daily active user
        expected_dau_data = [
            ('2024-12-01', 1),
            ('2024-12-02', 2),
            ('2024-12-03', 3)
        ]
        expected_dau_df = spark.createDataFrame(expected_dau_data, ['interaction_date', 'dau'])

        # Expected output for Monthly active user
        expected_mau_data = [
            (2024, 12, 3)
        ]
        expected_mau_df = spark.createDataFrame(expected_mau_data, ['year', 'month', 'mau'])

        # Run the function
        dau_df, mau_df = calculate_dau_and_mau(self.df)

        # Drop the 'created_at' and 'updated_at' columns before comparison
        dau_df = dau_df.drop('created_at', 'updated_at')
        mau_df = mau_df.drop('created_at', 'updated_at')

        # Check for equality using subtract()
        dau_diff = dau_df.subtract(expected_dau_df)
        mau_diff = mau_df.subtract(expected_mau_df)

        self.assertTrue(dau_diff.count() == 0, "DAU DataFrame does not match the expected output.")
        self.assertTrue(mau_diff.count() == 0, "MAU DataFrame does not match the expected output.")


    def test_calculate_sessions(self):
        # Expected output for session data
        expected_session_data = [
            (1, '1-1', 2, 560.0),
            (1, '1-2', 1, 560.0),
            (1, '1-3', 1, 560.0),
            (2, '2-1', 1, 100.0),
            (3, '3-1', 1, 200.0),
            (3, '3-2', 1, 200.0)
        ]
        expected_session_df = spark.createDataFrame(expected_session_data, 
                                                    ['user_id', 'session_id', 'actions_per_session', 'avg_session_duration_seconds'])

        # Run the function
        session_df = calculate_sessions(self.df)
        # Drop the 'created_at' and 'updated_at' columns before comparison
        session_df = session_df.drop('created_at', 'updated_at')

        # Check for equality using subtract()
        session_diff = session_df.subtract(expected_session_df)

        # Validate session result
        self.assertTrue(session_diff.count() == 0, "Session analysis DataFrame does not match the expected output.")

    def test_join_dataframe(self):
        # Create a second DataFrame with metadata
        metadata_data = [
            (1, "Kevin"),
            (2, "Jane"),
            (3, "Alice")
        ]
        metadata_columns = ["user_id", "user_name"]
        metadata_df = spark.createDataFrame(metadata_data, metadata_columns)

        # Expected output for the joined data
        expected_joined_data = [
            (1, "2024-12-01 10:00:00", 300000, "create", "Kevin"),
            (1, "2024-12-01 10:18:00", 200000, "share", "Kevin"),
            (1, "2024-12-02 09:00:00", 300000, "page_view", "Kevin"),
            (3, "2024-12-02 10:00:00", 200000, "share", "Alice"),
            (2, "2024-12-03 14:00:00", 100000, "page_view", "Jane"),
            (3, "2024-12-03 10:00:00", 200000, "delete", "Alice"),
            (1, "2024-12-03 15:00:00", 100000, "page_view", "Kevin")
        ]
        expected_joined_df = spark.createDataFrame(expected_joined_data, 
                                                  ['user_id', 'user_name', 'timestamp', 'duration_ms', 'action_type'])

        # Run the function
        joined_df = join_dataframe(self.df, metadata_df)

        # Validate the joined DataFrame result
        self.assertTrue(joined_df.collect() == expected_joined_df.collect(),
                        "Joined DataFrame does not match the expected output.")

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session
        spark.stop()

# Run the tests
if __name__ == "__main__":
    unittest.main()
