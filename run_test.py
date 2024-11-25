import unittest
from pyspark.sql import SparkSession
from test_cases import ETLTestCases
import config  

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ETLTest") \
        .master("local[2]") \
        .getOrCreate()

    # Inject the Spark session and config into the test case class
    ETLTestCases.spark = spark
    ETLTestCases.config = config

  
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(ETLTestCases))

    spark.stop()
