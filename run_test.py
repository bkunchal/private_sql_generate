import unittest
from pyspark.sql import SparkSession
from test_cases import ETLTestCases
from configs import queries  # Import the specific config file
import logging
import os

if __name__ == "__main__":
    # File paths for the sample data
    file_paths = {
        "my_table": "test/test-data/sampledata.csv",  # Ensure this file exists
    }

    # Validate file paths
    for table_name, file_path in file_paths.items():
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file for '{table_name}' not found: {file_path}")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETLTest_Config1") \
        .master("local[2]") \
        .getOrCreate()

    # Set up logger
    logger = logging.getLogger("ETLTestLogger")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Inject Spark session, queries, file paths, and logger into test cases
    ETLTestCases.spark = spark
    ETLTestCases.config = {"queries": queries}  # Pass `queries` from config1
    ETLTestCases.file_paths = file_paths
    ETLTestCases.logger = logger 

    # Run tests
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    # Stop Spark session
    spark.stop()