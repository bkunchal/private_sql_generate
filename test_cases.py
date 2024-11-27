import unittest
from src.extract_data import extract_data_lumi
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.spark = None
        cls.config = None
        cls.file_paths = None
        cls.logger = None

    def _load_sample_data(self):

        for table_name, file_path in self.file_paths.items():
            abs_path = file_path
            if not abs_path:
                raise FileNotFoundError(f"Data file not found: {file_path}")

            # Load the file into a Spark DataFrame and create a temporary view
            df = self.spark.read.csv(abs_path, header=True, inferSchema=True)
            df.createOrReplaceTempView(table_name)

    def test_etl_pipeline(self):

        # Load sample data
        self._load_sample_data()

        # Run extraction
        extract_data_lumi(self.spark, self.logger, self.config.queries["extract"])

        # Validate extraction views
        for query in self.config.queries["extract"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"View '{query['name']}' was not created"
            )

        # Run transformations
        execute_transform_queries(self.spark, self.config.queries, self.logger)

        # Validate transformation views
        for query in self.config.queries["transform"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"View '{query['name']}' was not created"
            )

    def test_load_simulation(self):

        # Get the load configuration
        load_config = self.config.queries["load"][0]
        dataframe_name = load_config["dataframe_name"]

        # Retrieve the final DataFrame
        try:
            final_df = self.spark.sql(f"SELECT * FROM {dataframe_name}")
            print("\nFinal DataFrame to be loaded into GBQ:")
            final_df.show()
        except Exception as e:
            self.fail(f"Failed to retrieve the final DataFrame '{dataframe_name}': {e}")
