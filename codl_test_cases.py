import unittest
import os
from pyspark.sql.types import FloatType

class ETLTestCases(unittest.TestCase):
    spark = None
    file_paths = None  # Dictionary of input files
    logger = None
    module_to_test = None  # PySpark module to test
    sql_variables = {}  # Mapping SQL variable names to expected view names
    output_validation_views = []  # List of views to validate (to be set in test module)

    @classmethod
    def setUpClass(cls):
        """
        Set up Spark session, logger, load input data, and create temp views before running tests.
        """
        assert cls.spark, "Spark session must be initialized before running tests."
        assert cls.file_paths, "Input file paths must be provided."
        assert cls.logger, "Logger must be initialized for debugging."
        assert cls.module_to_test, "PySpark module to test must be provided."

        # Validate file paths
        cls.validate_file_paths()

        # Load sectioned CSV data into Spark temporary views
        cls.load_data()

        # Pre-execute SQL queries to create temp views before tests
        cls.preload_temp_views()

    @classmethod
    def validate_file_paths(cls):
        """
        Validates that all provided file paths exist.
        """
        for file_name, file_path in cls.file_paths.items():
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Data file '{file_path}' not found.")
        cls.logger.info("All input files validated successfully.")

    @classmethod
    def replace_bq_types_for_spark(cls, query: str) -> str:
        """
        Efficiently replaces BigQuery-specific types and functions with Spark-friendly equivalents.
        - INT64         -> BIGINT
        - FLOAT64       -> DOUBLE
        - NUMERIC       -> DECIMAL(38,9)
        - BIGNUMERIC    -> DECIMAL(38,9)
        - TIME          -> STRING
        - DATETIME      -> TIMESTAMP
        - CURRENT_TIME() -> date_format(current_timestamp(), 'HH:mm:ss')
        """

        replacements = {
            "INT64": "BIGINT",
            "FLOAT64": "DOUBLE",
            "BIGNUMERIC": "DECIMAL(38,9)",
            "NUMERIC": "DECIMAL(38,9)",
            "TIME": "STRING",
            "DATETIME": "TIMESTAMP",
            "CURRENT_TIME()": "date_format(current_timestamp(), 'HH:mm:ss')"  # Extracts only time
        }

        for bq_type, spark_type in replacements.items():
            query = query.replace(bq_type, spark_type)

        return query

    @classmethod
    def load_data(cls):
        """
        Reads multiple sectioned CSV files and loads data into Spark temp views.
        """
        for file_name, file_path in cls.file_paths.items():
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
                            cls.create_temp_view(current_table, table_data)
                            table_data = []
                        current_table = line.strip("[]")  # Extract the table name
                    elif current_table and line:
                        table_data.append(line)

                # Process the last table's data
                if current_table and table_data:
                    cls.create_temp_view(current_table, table_data)

            except Exception as e:
                cls.logger.error(f"Failed to process sectioned file '{file_path}': {e}")
                raise RuntimeError(f"Error processing file '{file_path}': {e}")

    @classmethod
    def create_temp_view(cls, table_name, table_data):
        """
        Converts a table section into a Spark DataFrame and creates a temporary view.
        """
        try:
            headers = table_data[0].split(",")  # First row as headers
            data = [row.split(",") for row in table_data[1:]]  # Remaining rows as data
            df = cls.spark.createDataFrame(data, schema=headers)
            df.createOrReplaceTempView(table_name)
            cls.logger.info(f"Temp view '{table_name}' created successfully.")
            df.show()  # Debugging purposes
        except Exception as e:
            cls.logger.error(f"Failed to create temp view for '{table_name}': {e}")
            raise

    @classmethod
    def preload_temp_views(cls):
        """
        Execute all SQL queries in the module and create temp views before running tests.
        This ensures that views are accessible when test cases run.
        """
        for sql_var, view_name in cls.sql_variables.items():
            original_query = getattr(cls.module_to_test, sql_var, None)
            assert original_query, f"SQL variable '{sql_var}' not found in the PySpark module."

            # Replace BigQuery-specific types and functions with Spark equivalents.
            fixed_query = cls.replace_bq_types_for_spark(original_query)

            cls.logger.info(f"Preloading SQL query '{sql_var}':\n"
                            f"--- Original (BigQuery) ---\n{original_query}\n"
                            f"--- After Replacement ---\n{fixed_query}\n")
            # Validate Spark syntax before execution
            cls.validate_sql_syntax(fixed_query)

            try:
                df = cls.spark.sql(fixed_query)
                df.createOrReplaceTempView(view_name)
                cls.logger.info(f"Preloaded view '{view_name}' successfully.")
            except Exception as e:
                raise RuntimeError(f"Failed to preload SQL view '{view_name}': {e}")

    @classmethod
    def validate_sql_syntax(cls, query):
        """
        Validates the SQL syntax by generating its logical plan without execution.
        """
        try:
            cls.spark.sql(query).explain()
            cls.logger.info("SQL syntax is valid.")
        except Exception as e:
            cls.logger.error(f"SQL syntax validation failed: {e}")
            raise

    def test_sql_execution(self):
        """
        Execute SQL queries defined in the PySpark module.
        """
        for sql_var, view_name in self.sql_variables.items():
            original_query = getattr(self.module_to_test, sql_var, None)
            assert original_query, f"SQL variable '{sql_var}' not found in the PySpark module."

            # Replace BigQuery-specific types and functions with Spark equivalents.
            fixed_query = self.replace_bq_types_for_spark(original_query)

            self.logger.info(f"Executing query for '{sql_var}':\n"
                             f"--- Original (BigQuery) ---\n{original_query}\n"
                             f"--- After Replacement ---\n{fixed_query}\n")
            # Validate Spark syntax before execution
            self.validate_sql_syntax(fixed_query)

            try:
                df = self.spark.sql(fixed_query)
                df.createOrReplaceTempView(view_name)
                self.logger.info(f"Temp view '{view_name}' created successfully.")
                df.show()
            except Exception as e:
                self.fail(f"Failed to execute query '{sql_var}': {e}")

    def test_output_validation(self):
        """
        Validate only the specified output views.
        """
        for view_name in self.output_validation_views:
            try:
                final_df = self.spark.sql(f"SELECT * FROM {view_name}")
                self.assertGreater(final_df.count(), 0, f"View '{view_name}' is empty.")
                self.logger.info(f"Validated view '{view_name}' with {final_df.count()} rows.")
                final_df.show()  # Only display if in the specified views
            except Exception as e:
                self.fail(f"Validation failed for view '{view_name}': {e}")
