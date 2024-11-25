import unittest
from src.extract_data import extract_data_lumi
from src.transform_data import execute_transform_queries

class ETLTestCases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.spark = None
        cls.config = None

    def _load_sample_data(self, file_paths):
        for table_name, file_path in file_paths.items():
            abs_path = file_path
            if not abs_path:
                raise FileNotFoundError(f"Data file not found: {file_path}")

            # Load the file into a Spark DataFrame and create a temporary view
            df = self.spark.read.csv(abs_path, header=True, inferSchema=True)
            df.createOrReplaceTempView(table_name)

    def test_etl_pipeline(self):

        self._load_sample_data(self.config.file_paths)

        extract_data_lumi(self.spark, self.config.queries["extract"])

        # Validate extraction views
        for query in self.config.queries["extract"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"View '{query['name']}' was not created"
            )


        execute_transform_queries(self.spark, self.config.queries)

        for query in self.config.queries["transform"]:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"View '{query['name']}' was not created"
            )
