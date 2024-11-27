import unittest
from src.extract_data import extract_data_lumi
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Initialize placeholders for Spark session, config, file paths, and logger.
        These will be set dynamically by the test runner.
        """
        cls.spark = None
        cls.config = None
        cls.file_paths = None
        cls.logger = None

    def _load_sample_data(self):
        """
        Load sample data from file paths into temporary Spark views.
        """
        # Validate that file_paths is not None and has entries
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
        # Validate that the config is properly initialized
        self.assertIsNotNone(self.config, "config is not initialized")
        self.assertIn("extract", self.config["queries"], "config does not contain 'extract' queries")
        self.assertIn("transform", self.config["queries"], "config does not contain 'transform' queries")

        # Load sample data
        self._load_sample_data()

        # Run extraction
        extract_data_lumi(self.spark, self.logger, self.config["queries"]["extract"])

        # Validate extracted views
        for query in self.config["queries"]["extract"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"Extracted view '{query['name']}' was not created"
            )

        # Run transformations
        execute_transform_queries(self.spark, self.config["queries"]["transform"], self.logger)

        # Validate transformed views
        for query in self.config["queries"]["transform"]:
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
        # Validate that the load configuration exists
        self.assertIn("load", self.config["queries"], "config does not contain 'load' queries")
        load_config = self.config["queries"]["load"][0]

        # Validate the presence of `dataframe_name` in the load configuration
        dataframe_name = load_config.get("dataframe_name")
        self.assertIsNotNone(dataframe_name, "dataframe_name is not specified in the load configuration")

        try:
            # Retrieve and validate the final DataFrame
            final_df = self.spark.sql(f"SELECT * FROM {dataframe_name}")
            self.assertGreater(final_df.count(), 0, f"The DataFrame '{dataframe_name}' is empty")
            self.logger.info(f"Successfully retrieved the final DataFrame '{dataframe_name}' with {final_df.count()} rows")
        except Exception as e:
            self.fail(f"Failed to retrieve the final DataFrame '{dataframe_name}': {e}")