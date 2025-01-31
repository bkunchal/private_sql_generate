import unittest
import logging
import os
from pyspark.sql import SparkSession
from tests.test_cases import ETLTestCases  # Import the test case class
import src.main.tablename.sample_config as sample_config  # Import the PySpark file to test

if __name__ == "__main__":
    # Define multiple input data files (each containing multiple table sections)
    file_paths = {
        "sample_data": "test-data/sample_data.csv"  # Ensure this file exists
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

    # Inject Spark session, logger, input data, and PySpark module into test cases
    ETLTestCases.spark = spark
    ETLTestCases.logger = logger
    ETLTestCases.file_paths = file_paths  # Pass sectioned CSV file paths
    ETLTestCases.module_to_test = sample_config  # Reference to the PySpark script
    ETLTestCases.sql_variables = {
        "customer_orders_query": "customer_orders_summary",
        "regional_sales_query": "regional_sales_summary",
        "product_sales_query": "product_sales_summary"
    }

    # Run the tests
    result = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    # Stop Spark session
    spark.stop()

    # Exit with failure code if tests fail
    if not result.wasSuccessful():
        exit(1)
