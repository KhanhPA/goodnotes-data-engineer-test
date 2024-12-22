import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DatasetGeneration") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.default.parallelism", 4) \
    .getOrCreate()

# Define schema for user_interactions
user_interactions_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("action_type", StringType(), False),
    StructField("page_id", StringType(), False),
    StructField("duration_ms", IntegerType(), False),
    StructField("app_version", StringType(), False)
])

# Define schema for user_metadata
user_metadata_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("join_date", StringType(), False),
    StructField("country", StringType(), False),
    StructField("device_type", StringType(), False),
    StructField("subscription_type", StringType(), False)
])

# Data generation functions
def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"

def generate_timestamp():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

def generate_action_type():
    return random.choice(['page_view', 'edit', 'create', 'delete', 'share'])

def generate_page_id():
    return f"p{random.randint(1, 1000000):06d}"

def generate_duration_ms():
    return random.randint(100, 300000)

def generate_app_version():
    major = random.randint(5, 7)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)
    return f"{major}.{minor}.{patch}"

def generate_join_date():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_country():
    return random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'MX'])

def generate_device_type():
    return random.choice(['iPhone', 'iPad', 'Android Phone', 'Android Tablet', 'Windows', 'Mac'])

def generate_subscription_type():
    return random.choice(['free', 'basic', 'premium', 'enterprise'])

# Generate data and save as Parquet
def generate_user_interactions(num_records, output_path):
    data = [
        (
            generate_user_id(),
            generate_timestamp().strftime("%Y-%m-%d %H:%M:%S"),
            generate_action_type(),
            generate_page_id(),
            generate_duration_ms(),
            generate_app_version()
        )
        for _ in range(num_records)
    ]
    df = spark.createDataFrame(data, schema=user_interactions_schema)
    df.write.mode("overwrite").parquet(output_path)
    print(f"user_interactions saved to {output_path}")

def generate_user_metadata(num_records, output_path):
    data = [
        (
            generate_user_id(),
            generate_join_date().strftime("%Y-%m-%d"),
            generate_country(),
            generate_device_type(),
            generate_subscription_type()
        )
        for _ in range(num_records)
    ]
    df = spark.createDataFrame(data, schema=user_metadata_schema)
    df.write.mode("overwrite").parquet(output_path)
    print(f"user_metadata saved to {output_path}")

# Generate and save datasets
generate_user_interactions(1000000, "user_interactions_sample.parquet")
generate_user_metadata(100000, "user_metadata_sample.parquet")

print("Sample datasets generated successfully in Parquet format.")