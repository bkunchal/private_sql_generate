import unittest
from sample_spark_config import queries
from extract_data import extract_data_lumi
from transform_data import execute_transform_queries
from load_data import load_data_to_lumi
from logger_util import get_logger


logger = get_logger(__name__)

class TestMainETLFramework(unittest.TestCase):
    spark = None 

    @classmethod
    def setUpClass(cls):
        logger.info("Setting up data for all tests")
        extract_data_lumi(cls.spark, queries["extract"], logger)
        execute_transform_queries(cls.spark,  queries["transform"], logger)
        logger.info("Data setup complete.")

   # Test that extract_data views are created.
    def test_extract_data_views(self):
        logger.info("Running test: test_extract_data_views")
        for query in queries["extract"]:
            view_name = query["name"]
            self.assertTrue(self.spark.catalog.tableExists(view_name), f"{view_name} view does not exist")
            logger.info(f"{view_name} view exists.")

# Test that transform_data views are created.
    def test_transform_data_views(self):
        logger.info("Running test: test_transform_data_views")
        for query in queries["transform"]:
            view_name = query["name"]
            self.assertTrue(self.spark.catalog.tableExists(view_name), f"{view_name} view does not exist")
            logger.info(f"{view_name} view exists.")

# Test that transformed data is loaded into the target GBQ table.
    def test_load_data(self):
        logger.info("Running test: test_load_data")
        load_data_to_lumi(self.spark, queries, logger)
        target_table = queries["load"][0]["table_name"]
        loaded_df = self.spark.read.format("bigquery").option("table", target_table).load()
        logger.info(f"Target table {target_table} loaded. Row count: {loaded_df.count()}")
