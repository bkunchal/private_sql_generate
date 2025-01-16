import unittest
from pyspark.sql import DataFrame
from src.transform_data import execute_transform_queries


class ETLTestCases(unittest.TestCase):
    spark = None
    config = None
    file_paths = None
    logger = None
    extract_key = "extract"  # Default key for extract module
    transform_key = "transform"  # Default key for transform module

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

    def load_sample_data(self):
        """
        Reads multiple input files with table sections and creates temporary views.
        """
        assert self.file_paths, "No file paths provided for loading data"

        for file_path in self.file_paths.values():
            self.process_sectioned_file(file_path)

    def process_sectioned_file(self, file_path):
        """
        Processes a single file containing multiple table sections and creates temporary views.
        """
        try:
            with open(file_path, "r") as file:
                lines = file.readlines()

            current_table = None
            table_data = []

            for line in lines:
                line = line.strip()
                if line.startswith("[") and line.endswith("]"):
                    # Process the previous table's data
                    if current_table and table_data:
                        self.create_temp_view(current_table, table_data)
                        table_data = []

                    current_table = line.strip("[]")  # Extract the table name
                elif current_table and line:
                    table_data.append(line)

            # Process the last table's data
            if current_table and table_data:
                self.create_temp_view(current_table, table_data)

        except Exception as e:
            self.logger.error(f"Failed to process sectioned file '{file_path}': {e}")
            raise RuntimeError(f"Error processing file '{file_path}': {e}")

    def create_temp_view(self, table_name, table_data):
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

    def validate_sql_syntax(self, query):
        """
        Validates SQL syntax by running the EXPLAIN command on the query.
        """
        try:
            self.spark.sql(f"EXPLAIN {query}")
            return True
        except Exception as e:
            return str(e)

    def test_extract_data_lumi(self):
        """
        Executes the "Extract" phase queries:
        - Handles dynamic_variable_flag for parameterized queries.
        - Validates that extracted views are created successfully.
        """
        extract_queries = self.config.get("queries", {}).get(self.extract_key, [])
        assert extract_queries, f"No queries found for key '{self.extract_key}' in config"

        # Parameters from the config or run_test.py
        dynamic_params = self.config.get("dynamic_params", {})  # Default empty dict

        for query_info in extract_queries:
            table_name = query_info["name"]
            query = query_info["query"]
            dynamic_variable_flag = query_info.get("dynamic_variable_flag", False)

            # Handle dynamic parameter substitution if the flag is True
            if dynamic_variable_flag:
                try:
                    query = query.format(**dynamic_params)
                except KeyError as e:
                    self.logger.error(f"Missing parameter {e} for dynamic query: {table_name}")
                    raise ValueError(f"Missing parameter {e} for dynamic query: {table_name}")

            # Validate SQL syntax
            syntax_error = self.validate_sql_syntax(query)
            if syntax_error is not True:
                raise RuntimeError(f"Syntax error in extract query '{table_name}': {syntax_error}")

            # Execute the query and create the temp view
            try:
                self.logger.info(f"Executing extract query for '{table_name}': {query}")
                df = self.spark.sql(query)
                df.createOrReplaceTempView(table_name)
                self.logger.info(f"Temp view '{table_name}' created successfully after extraction")
            except Exception as e:
                self.logger.error(f"Failed to execute extract query for '{table_name}': {e}")
                raise

        # Validate extracted views
        for query_info in extract_queries:
            table_name = query_info["name"]
            self.assertTrue(
                self.spark.catalog.tableExists(table_name),
                f"Extracted view '{table_name}' was not created"
            )

    def test_etl_pipeline(self):
        """
        Executes the ETL pipeline:
        - Loads sample data
        - Runs extract queries based on `extract_key`
        - Validates and runs transformation queries based on `transform_key`
        """
        self.load_sample_data()
        self.test_extract_data_lumi()

        queries = self.config.get("queries", {})
        transform_queries = queries.get(self.transform_key, [])

        # Validate transformation queries
        for query_info in transform_queries:
            table_name = query_info["name"]
            query = query_info["query"]

            syntax_error = self.validate_sql_syntax(query)
            if syntax_error is not True:
                raise RuntimeError(f"Syntax error in transform query '{table_name}': {syntax_error}")

        # Execute transformation queries using the provided method
        execute_transform_queries(self.spark, transform_queries, self.logger)

        # Validate transformed views
        for query_info in transform_queries:
            table_name = query_info["name"]
            self.assertTrue(
                self.spark.catalog.tableExists(table_name),
                f"Transformed view '{table_name}' was not created"
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
