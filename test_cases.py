import unittest
from pyspark.sql import DataFrame
from src.transform_data import transform_queries


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


def execute_extraction(self, extract_queries, dynamic_params):
    """
    Executes extraction queries and creates temp views.
    """
    self.logger.info("Starting extraction phase.")

    for query_info in extract_queries:
        table_name = query_info["name"]
        query = query_info["query"]
        dynamic_variable_flag = query_info.get("dynamic_variable_flag", False)

        try:
            if dynamic_variable_flag:
                self.logger.info(f"Executing dynamic query for '{table_name}': {query} with params: {dynamic_params}")
                formatted_query = query.format(**dynamic_params)
            else:
                self.logger.info(f"Executing static query for '{table_name}': {query}")
                formatted_query = query

            # Execute query
            df = self.spark.sql(formatted_query)
            self.logger.info(f"Schema of temp view '{table_name}':")
            df.printSchema()
            self.logger.info(f"Row count for temp view '{table_name}': {df.count()}")
            
            # Create temp view
            df.createOrReplaceTempView(table_name)
            self.logger.info(f"Temp view '{table_name}' created successfully.")
        
        except Exception as e:
            self.logger.error(f"Failed to execute query for '{table_name}': {e}")
            raise

    self.logger.info("Completed extraction phase.")


def rename_views(self, extract_queries):
    """
    Renames the extracted temp views to validated_<table_name>.
    """
    self.logger.info("Starting renaming of views to validated_<table_name>.")

    for query_info in extract_queries:
        table_name = query_info["name"]
        validated_table_name = f"validated_{table_name}"

        try:
            # Check if the table exists
            if not self.spark.catalog.tableExists(table_name):
                self.logger.warning(f"Temp view '{table_name}' does not exist. Skipping rename.")
                continue

            # Rename the view
            self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {validated_table_name} AS SELECT * FROM {table_name}")
            self.logger.info(f"Renamed temp view '{table_name}' to '{validated_table_name}'.")
        
        except Exception as e:
            self.logger.error(f"Failed to rename view '{table_name}' to '{validated_table_name}': {e}")
            raise

    self.logger.info("Completed renaming of views.")




def test_extract_data_lumi(self):
    """
    Executes the extraction and renaming process.
    """
    self.logger.info("Starting the extract and rename module.")

    # Get extract queries from config
    extract_queries = self.config.get("queries", {}).get(self.extract_key, [])
    assert extract_queries, f"No queries found for key '{self.extract_key}' in config"

    # Parameters from the config or run_test.py
    dynamic_params = self.config.get("params", {})  # Default empty dictionary
    self.logger.debug(f"Dynamic parameters: {dynamic_params}")

    # Step 1: Extract Views
    self.extract_views(extract_queries, dynamic_params)

    # Step 2: Rename Views
    self.rename_views(extract_queries)

    self.logger.info("Completed the extract and rename module.")




def test_etl_pipeline(self):
    """
    Executes the ETL pipeline:
    - Loads sample data
    - Executes extraction queries
    - Executes transformation queries using the imported function
    """
    self.load_sample_data()
    self.test_extract_data_lumi()

    # Fetch the transformation queries from the configuration
    transform_queries_config = self.config.get("queries", {}).get(self.transform_key, [])
    assert transform_queries_config, f"No queries found for key '{self.transform_key}' in config"

    # Use the `transform_queries` function from the imported module
    try:
        transform_queries(self.spark, self.logger, transform_queries_config, params=self.config.get("params"))
    except Exception as e:
        self.logger.error(f"Failed to execute transformation queries: {str(e)}")
        raise

    # Validate transformed views
    for query_info in transform_queries_config:
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
