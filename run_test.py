import unittest
from pyspark.sql import SparkSession
from test_cases import TestMainETLFramework
from logger_util import get_logger


logger = get_logger(__name__)

if __name__ == "__main__":
    # Build the Spark session
    logger.info("Initializing Spark session...")
    spark = (
        SparkSession.builder
        .appName("ETLTest")
        .getOrCreate()
    )
    spark.conf.set("temporaryGcsBucket", "prj-d-gcsde-odl")
    spark.conf.set("viewsEnabled", "true")
    spark.conf.set("materializationDataset", "data")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    logger.info("Spark session initialized.")

    TestMainETLFramework.spark = spark

    # Load and run the test suite
    logger.info("Loading test suite...")
    suite = unittest.TestLoader().loadTestsFromTestCase(TestMainETLFramework)
    runner = unittest.TextTestRunner(verbosity=2)
    logger.info("Running test suite...")
    result = runner.run(suite)
    logger.info("Stopping Spark session...")
    spark.stop()
    logger.info("Spark session stopped.")

    exit_code = 0 if result.wasSuccessful() else 1
    logger.info(f"Test suite completed. Exit code: {exit_code}")
    exit(exit_code)
