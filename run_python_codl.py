import unittest
import logging
from pyspark.sql import SparkSession
from tests.test_cases import ETLTestCases
import src.main.tablename.sample_config as sample_config  # Import the PySpark file

if __name__ == "__main__":
    # Define multiple input data files (each containing multiple table sections)
    file_paths = {
        "file1": "test-data/file1.csv",
        "file2": "test-data/file2.csv",
    }

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETLTest") \
        .master("local[*]") \
        .getOrCreate()

    # Set up logger
    logger = logging.getLogger("ETLTestLogger")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Configure the test case with Spark, logger, and input data files
    ETLTestCases.spark = spark
    ETLTestCases.logger = logger
    ETLTestCases.file_paths = file_paths  # Only passing file paths
    ETLTestCases.module_to_test = sample_config  # Reference to the PySpark script
    ETLTestCases.sql_variables = {
        "sus_retail_temp_Query": "sus_retail_temp",
        "sus_retail_final_Query": "sus_retail_final"
    }

    # Run the tests
    result = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    # Stop Spark session
    spark.stop()

    # Exit with failure code if tests fail
    if not result.wasSuccessful():
        exit(1)
