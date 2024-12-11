import unittest
from pyspark.sql import DataFrame
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    spark = None
    config = None
    file_paths = None
    logger = None

    @classmethod
    def setUpClass(cls):
        """
        Validates the essential class-level attributes before running any tests.
        """
        print("DEBUG: setUpClass - Config:", cls.config)  # Debugging config
        assert cls.config, "Config must be set before running tests"
        assert cls.spark, "Spark session must be set before running tests"
        assert cls.file_paths, "File paths must be set before running tests"
        assert cls.logger, "Logger must be set before running tests"

    def _load_sample_data(self):
        """
        Parses a single input CSV file containing multiple sections, splitting it into temporary views.
        """
        input_file_path = self.file_paths.get("input_data")
        if not input_file_path:
            raise FileNotFoundError("Input data file is not provided.")

        try:
            with open(input_file_path, "r") as file:
                lines = file.readlines()

            current_table = None
            table_data = []

            for line in lines:
                line = line.strip()
                if line.startswith("[") and line.endswith("]"):
                    # Process the previous table's data
                    if current_table and table_data:
                        self._create_temp_view(current_table, table_data)
                        table_data = []

                    current_table = line.strip("[]")  # Extract the table name
                elif current_table and line:
                    table_data.append(line)

            # Process the last table's data
            if current_table and table_data:
                self._create_temp_view(current_table, table_data)

        except Exception as e:
            raise RuntimeError(f"Failed to load input data: {e}")

    def _create_temp_view(self, table_name, table_data):
        """
        Converts a table section into a Spark DataFrame and creates a temporary view.
        """
        try:
            # Split the first row as headers and the subsequent rows as data
            headers = table_data[0].split(",")
            data = [row.split(",") for row in table_data[1:]]
            df = self.spark.createDataFrame(data, schema=headers)
            df.createOrReplaceTempView(table_name)

            self.logger.info(f"Temp view '{table_name}' created successfully")
            self.logger.info(f"Schema for '{table_name}': {df.schema}")
            df.show()
        except Exception as e:
            self.logger.error(f"Failed to create temp view for '{table_name}': {e}")
            raise

    def _validate_sql_syntax(self, query):
        """
        Validates SQL syntax by running the EXPLAIN command on the query.
        """
        try:
            self.spark.sql(f"EXPLAIN {query}")
            return True
        except Exception as e:
            return str(e)

    def _mock_extract_data_lumi(self):
        """
        Simulates the "Extract" phase by executing extract queries on the temporary views.
        """
        extract_queries = self.config.get("queries", {}).get("extract", [])
        assert extract_queries, "No extract queries found in config"

        for query_info in extract_queries:
            table_name = query_info["name"]
            query = query_info["query"]

            syntax_error = self._validate_sql_syntax(query)
            if syntax_error is not True:
                raise RuntimeError(f"Syntax error in extract query '{table_name}': {syntax_error}")

            try:
                self.logger.info(f"Executing extract query for '{table_name}': {query}")
                df = self.spark.sql(query)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Temp view '{table_name}' created successfully after extraction")
            except Exception as e:
                self.logger.error(f"Failed to execute extract query for '{table_name}': {e}")
                raise

    def test_etl_pipeline(self):
        """
        Executes the ETL pipeline:
        - Loads sample data
        - Runs extract queries
        - Validates transformations
        """
        self._load_sample_data()
        self._mock_extract_data_lumi()

        queries = self.config.get("queries", {})
        extract_queries = queries.get("extract", [])
        transform_queries = queries.get("transform", [])

        # Validate extracted views
        for query in extract_queries:
            table_name = query["name"]
            self.assertTrue(
                self.spark.catalog.tableExists(table_name),
                f"Extracted view '{table_name}' was not created"
            )

        # Validate and execute transformations
        for query in transform_queries:
            syntax_error = self._validate_sql_syntax(query["query"])
            if syntax_error is not True:
                raise RuntimeError(f"Syntax error in transform query '{query['name']}': {syntax_error}")

        execute_transform_queries(self.spark, transform_queries, self.logger)

        # Validate transformed views
        for query in transform_queries:
            self.assertTrue(
                self.spark.catalog.tableExists(query["name"]),
                f"Transformed view '{query['name']}' was not created"
            )

    def test_load_simulation(self):
        """
        Simulates the "Load" phase by verifying the final DataFrame.
        """
        queries = self.config.get("queries", {})
        load_config = queries.get("load", [{}])[0]

        dataframe_name = load_config.get("dataframe_name")
        assert dataframe_name, "dataframe_name is not specified in the load configuration"

        try:
            final_df = self.spark.sql(f"SELECT * FROM {dataframe_name}")
            self.assertGreater(final_df.count(), 0, f"The DataFrame '{dataframe_name}' is empty")
            self.logger.info(f"Final DataFrame '{dataframe_name}' retrieved successfully with {final_df.count()} rows")
        except Exception as e:
            self.fail(f"Failed to retrieve the final DataFrame '{dataframe_name}': {e}")
