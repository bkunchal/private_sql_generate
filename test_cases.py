import unittest
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    spark = None
    config = None
    file_paths = None
    logger = None

    @classmethod
    def setUpClass(cls):
        print("DEBUG: setUpClass - Config:", cls.config)  # Debugging config
        assert cls.config is not None, "config must be set before running tests"
        assert cls.spark is not None, "Spark session must be set before running tests"
        assert cls.file_paths is not None, "file_paths must be set before running tests"
        assert cls.logger is not None, "logger must be set before running tests"

    def _load_sample_data(self):
        self.assertIsNotNone(self.file_paths, "file_paths is not initialized")
        self.assertGreater(len(self.file_paths), 0, "file_paths is empty")

        for table_name, file_path in self.file_paths.items():
            if not file_path:
                raise FileNotFoundError(f"Data file for '{table_name}' is missing or not set")
            try:
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Loaded data for table '{table_name}' from '{file_path}'")
                self.logger.info(f"Schema of table '{table_name}': {df.schema}")
                df.show()
            except Exception as e:
                raise RuntimeError(f"Failed to load data for table '{table_name}': {e}")

    def _mock_extract_data_lumi(self):
        self.assertIsNotNone(self.config, "config is not initialized")
        extract_queries = self.config.get("queries", {}).get("extract", [])
        self.assertGreater(len(extract_queries), 0, "No extract queries found in config")

        for query_info in extract_queries:
            table_name = query_info["name"]
            query = query_info["query"]

            try:
                self.logger.info(f"Checking columns in 'my_table':")
                self.spark.sql("SELECT * FROM my_table").show()
                # Execute the query on the pre-loaded temporary views
                self.logger.info(f"Executing extract query for '{table_name}': {query}")
                df = self.spark.sql(query)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Temp view created for extracted table '{table_name}'")
            except Exception as e:
                raise RuntimeError(f"Failed to execute extract query for '{table_name}': {e}")

    def test_etl_pipeline(self):
        print("DEBUG: test_etl_pipeline - Config:", self.config)
        self.assertIsNotNone(self.config, "config is not initialized")
        queries = self.config.get("queries", {})
        self.assertIn("extract", queries, "config does not contain 'extract' queries")
        self.assertIn("transform", queries, "config does not contain 'transform' queries")

        self._load_sample_data()

        self._mock_extract_data_lumi()

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
