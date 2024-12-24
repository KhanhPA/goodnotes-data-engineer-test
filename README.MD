# README: Data Processing Application with PySpark

## Overview
This repository implements a Spark-based application designed to process user interaction data for analytics. It provides the following features:
- **Daily Active Users (DAU)** calculation.
- **Monthly Active Users (MAU)** calculation.
- **Session-Based Analysis**, including session segmentation and duration calculation.
- **Joining User Interaction Data with User Metadata** for joining analysis.
- **Optimization for Production** by analyzing Spark metrics, improving execution plans, and reducing runtime overhead.

The repository also includes unit tests to validate the correctness of implemented functions.

---

## Repository Structure
```
|-- config/
|   |-- spark_config.py  # Contains the Spark configuration.
|
|-- src/
|   |-- analytic_functions.py  # Core data processing functions.
|   |-- utils.py  # Utility functions for common operations.
|
|-- data/
|   |-- csv/         # Input data in CSV format.
|   |-- parquet/     # Input data in Parquet format.
|
|-- output/          # Output data storage location.
|
|-- main.py          # Main script to execute the Spark application.
|
|-- unit_test.py     # Unit tests for core functions in src.
|
|-- README.md        # Project documentation (this file).
|
|-- requirements.txt # Project dependencies.
```

---

## Setup Instructions

### Prerequisites
1. Python 3.8 or later.
2. Apache Spark 3.0 or later.
3. `pip` for Python package management.
4. A virtual environment (optional, but recommended).

### Installation Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/KhanhPA/goodnotes-data-engineer-test.git
   cd goodnotes-data-engineer-test
   ```

2. Set up a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## Running the Application

### Run the Main Script
To execute the application, run the `main.py` script:
```bash
python main.py
```

### Check Output
Processed data will be available in the `output/` directory.

---

## Running Unit Tests

1. Ensure the virtual environment is active (if using one).

2. Run the unit tests using the following command:
   ```bash
   python -m unittest unit_test.py
   ```

3. Confirm that all tests pass successfully.

---

## Explanation of the Approach

### Data Processing Functions
The `src/analytic_functions.py` file includes functions for:
- **Calculating DAU/MAU**: Extracting active user counts daily and monthly based on user interactions.
- **Session-Based Analysis**: Determining sessions, session duration, and average actions per session. The calculation process will follow these steps:
    + Step 1: Calculate the action end time for each action, this will add the duration_ms to the start time of the interaction
    + Step 2: Gaps and Islands - Find the previous endtime of each interactions, and then evaluate if there are any continuous duration for the same session (1 session is defined by if the user is active and their session window gaps is no more than 20 minutes between each session of interaction). 
    + Step 3: Group the interactions within the same session and count for the total actions that the user have done in those sessions
    + Step 4: Calculate the average session duration by = (total duration of every session/the number of sessions the user have)
- **Joining User Data**: Tracks users metadata for each interactions.

### Optimization Strategy
- **Spark UI Analysis**: Monitored execution plans, task durations, and job stages to identify bottlenecks.
    + Remove uneccessary operations like showString (show the output dataframe to the terminal).
    + Change the input file format to Parquet (columnar format to optimize the read operation and also smaller file input).
    + Choose the correct number of partitions to handle the job runs effectively (small dataset).
- **Broadcast Joins**: Optimized small lookup tables to minimize shuffle operations.
- **Partitioning**: Partitioned input and output datasets to parallelize processing and optimize storage.
- **Salting**: Used salting techniques for join keys to address skewed data distributions.


### Unit Tests
- Tested individual functions in isolation.
- Mocked small datasets to validate results against expected outputs.
- Excluded non-essential columns during testing (e.g., `created_at`, `updated_at`) to focus on relevant logic.
- For Joining datasets task, as the optimization process introduce Salting strategy, so a very small test cases in the unit test is not sufficient to run this, so the all the optimization for joining will be commented out and will be showed in the recording the this particular function.

### Orchestration
- Implement automated deployment pipelines for production. This can be done via Apache Airflow to set up Dags to orchestrate the spark job run on a cron/triggered schedules
