import unittest
from pyspark.sql import SparkSession
from test_cases import ETLTestCases
import logging
import os
import pyspark_file  # Replace with the name of your provided PySpark file

if __name__ == "__main__":
    # File path for the sample sectioned data
    file_path = "test/test-data/sampledata.csv"  # Path to your input data file

    # Validate the existence of the file
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")

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

    # Configure the ETL test case class
    ETLTestCases.spark = spark
    ETLTestCases.logger = logger
    ETLTestCases.file_path = file_path  # Input file path
    ETLTestCases.module_to_test = pyspark_file  # Reference to the provided PySpark script
    ETLTestCases.sql_variables = {
        "sus_retail_temp_Query": "sus_retail_temp",
        "sus_retail_final_Query": "sus_retail_final"
    }

    # Run the tests
    result = unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    # Stop the Spark session
    spark.stop()

    # Exit with the appropriate status
    if not result.wasSuccessful():
        exit(1)
