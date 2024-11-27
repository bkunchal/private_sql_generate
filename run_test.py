import unittest
from pyspark.sql import SparkSession
from tests.test_cases import ETLTestCases
from configs import config1  # Import the specific config file
import logging

if __name__ == "__main__":
    # File paths for the sample data
    file_paths = {
        "extract_data": "data/test_extract_data.csv",
        "another_table": "data/another_table.csv"
    }

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

    # Inject Spark session, config, file paths, and logger into test cases
    ETLTestCases.spark = spark
    ETLTestCases.config = config1
    ETLTestCases.file_paths = file_paths
    ETLTestCases.logger = logger 

    # Run tests
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    # Stop Spark session
    spark.stop()
