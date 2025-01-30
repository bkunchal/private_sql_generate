import unittest


class ETLTestCases(unittest.TestCase):
    spark = None
    file_path = None  # Input sectioned file path
    logger = None
    module_to_test = None  # Dynamically loaded PySpark module
    sql_variables = {}  # Dictionary mapping SQL variable names to expected view names

    @classmethod
    def setUpClass(cls):
        """
        Set up Spark session, logger, and load input data.
        """
        assert cls.spark, "Spark session must be initialized before running tests."
        assert cls.file_path, "Input file path must be provided for tests."
        assert cls.logger, "Logger must be initialized for debugging."
        assert cls.module_to_test, "PySpark module to test must be provided."

        # Load sectioned CSV data into Spark temporary views
        cls.load_sectioned_data()

    @classmethod
    def load_sectioned_data(cls):
        """
        Reads a single sectioned CSV file and loads data into Spark temp views.
        """
        try:
            with open(cls.file_path, "r") as file:
                lines = file.readlines()

            current_table = None
            table_data = []

            for line in lines:
                line = line.strip()
                if line.startswith("[") and line.endswith("]"):
                    # Process the current table's data
                    if current_table and table_data:
                        cls.create_temp_view(current_table, table_data)
                        table_data = []

                    current_table = line.strip("[]")  # Extract table name
                elif current_table and line:
                    table_data.append(line)

            # Process the last table's data
            if current_table and table_data:
                cls.create_temp_view(current_table, table_data)

        except Exception as e:
            cls.logger.error(f"Error processing sectioned file '{cls.file_path}': {e}")
            raise RuntimeError(f"Failed to process file '{cls.file_path}': {e}")

    @classmethod
    def create_temp_view(cls, table_name, table_data):
        """
        Create a temporary view for a table section.
        """
        try:
            headers = table_data[0].split(",")  # First row as headers
            data = [row.split(",") for row in table_data[1:]]  # Remaining rows as data
            df = cls.spark.createDataFrame(data, schema=headers)
            df.createOrReplaceTempView(table_name)

            cls.logger.info(f"Temp view '{table_name}' created successfully.")
            df.show()  # Debugging purposes
        except Exception as e:
            cls.logger.error(f"Failed to create temp view '{table_name}': {e}")
            raise RuntimeError(f"Error creating temp view '{table_name}': {e}")

    def test_sql_execution(self):
        """
        Execute SQL queries defined in the PySpark module.
        """
        for sql_var, view_name in self.sql_variables.items():
            query = getattr(self.module_to_test, sql_var, None)
            assert query, f"SQL variable '{sql_var}' not found in the PySpark module."

            try:
                self.logger.info(f"Executing query for '{sql_var}': {query}")
                df = self.spark.sql(query)
                df.createOrReplaceTempView(view_name)
                self.logger.info(f"Temp view '{view_name}' created successfully.")
                df.show()
            except Exception as e:
                self.fail(f"Failed to execute query '{sql_var}': {e}")

    def test_output_validation(self):
        """
        Validate the final output view.
        """
        for sql_var, view_name in self.sql_variables.items():
            try:
                final_df = self.spark.sql(f"SELECT * FROM {view_name}")
                self.assertGreater(final_df.count(), 0, f"View '{view_name}' is empty.")
                self.logger.info(f"Validated view '{view_name}' with {final_df.count()} rows.")
            except Exception as e:
                self.fail(f"Validation failed for view '{view_name}': {e}")
