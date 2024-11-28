import unittest
from src.extract_data import extract_data_lumi
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    spark = None
    config = None
    file_paths = None
    logger = None

    @classmethod
    def setUpClass(cls):
        """
        Ensure class attributes are correctly initialized.
        """
        print("DEBUG: setUpClass - Config:", cls.config)  # Debugging config
        assert cls.config is not None, "config must be set before running tests"
        assert cls.spark is not None, "Spark session must be set before running tests"
        assert cls.file_paths is not None, "file_paths must be set before running tests"
        assert cls.logger is not None, "logger must be set before running tests"

    def _load_sample_data(self):
        """
        Load sample data from file paths into temporary Spark views.
        """
        self.assertIsNotNone(self.file_paths, "file_paths is not initialized")
        self.assertGreater(len(self.file_paths), 0, "file_paths is empty")

        for table_name, file_path in self.file_paths.items():
            if not file_path:
                raise FileNotFoundError(f"Data file for '{table_name}' is missing or not set")
            
            # Load the CSV file into a Spark DataFrame
            try:
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Loaded data for table '{table_name}' from '{file_path}'")
            except Exception as e:
                raise RuntimeError(f"Failed to load data for table '{table_name}': {e}")

    def test_etl_pipeline(self):
        """
        Test the full ETL pipeline:
        - Load sample data
        - Run extract queries
        - Run transform queries
        """
        print("DEBUG: test_etl_pipeline - Config:", self.config)
        self.assertIsNotNone(self.config, "config is not initialized")
        queries = self.config.get("queries", {})
        self.assertIn("extract", queries, "config does not contain 'extract' queries")
        self.assertIn("transform", queries, "config does not contain 'transform' queries")

        # Load sample data
        self._load_sample_data()

        # Run extraction
        extract_data_lumi(self.spark, self.logger, queries["extract"])

        # Validate extracted views
        for query in queries["extract"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"Extracted view '{query['name']}' was not created"
            )

        # Run transformations
        execute_transform_queries(self.spark, queries["transform"], self.logger)

        # Validate transformed views
        for query in queries["transform"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"Transformed view '{query['name']}' was not created"
            )

    def test_load_simulation(self):
        """
        Test the load simulation:
        - Validate the final DataFrame is created
        - Validate the load configuration
        """
        print("DEBUG: test_load_simulation - Config:", self.config)
        queries = self.config.get("queries", {})
        self.assertIn("load", queries, "config does not contain 'load' queries")
        load_config = queries["load"][0]

        dataframe_name = load_config.get("dataframe_name")
        self.assertIsNotNone(dataframe_name, "dataframe_name is not specified in the load configuration")

        try:
            final_df = self.spark.sql(f"SELECT * FROM {dataframe_name}")
            self.assertGreater(final_df.count(), 0, f"The DataFrame '{dataframe_name}' is empty")
            self.logger.info(f"Successfully retrieved the final DataFrame '{dataframe_name}' with {final_df.count()} rows")
        except Exception as e:
            self.fail(f"Failed to retrieve the final DataFrame '{dataframe_name}': {e}")