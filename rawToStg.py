import argparse
import yaml
import logging
import os
from sqlGenerator import generate_sql_from_yaml_file
from logger import setup_logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def create_spark_session():
    # Get environment variables or default values
    app_name = os.getenv("SPARK_APP_NAME", "Load SQL to BigQuery")
    
    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    return spark


def load_sql_to_bigquery(sql_query, staging_table_name, load_type, logger):
    spark = create_spark_session()

    try:
        # Execute SQL query and load into a DataFrame
        df = spark.read \
            .format("bigquery") \
            .option("query", sql_query) \
            .load()
    except Exception as e:
        logger.error(f"Error executing SQL query: {e}")
        return

    # Cache the DataFrame to improve performance on multiple operations
    df.cache()


    # Add the load_ts column with the current timestamp
    df = df.withColumn("load_ts", current_timestamp())

    # Check if the table exists in BigQuery
    table_exists = spark._jvm.com.google.cloud.bigquery.BigQuery.createOrGetDataset().tableExists(staging_table_name)

    # If the table doesn't exist, create it based on the DataFrame schema
    if not table_exists:
        # Extract the schema from the DataFrame
        schema = df.schema.simpleString()  # Get schema as a string
        create_table_query = f"CREATE TABLE {staging_table_name} ({schema}, load_ts TIMESTAMP)"
        try:
            spark.sql(create_table_query)
            logger.info(f"Created table '{staging_table_name}' with schema: {schema}")
        except Exception as e:
            logger.error(f"Error creating table in BigQuery: {e}")
            return

    # Truncate the table if load_type is 'truncate'
    if load_type == 'truncate':
        try:
            # Count the number of records before truncating
            count_before_truncate = spark.sql(f"SELECT COUNT(*) AS count FROM {staging_table_name}").collect()[0]['count']
            spark.sql(f"TRUNCATE TABLE {staging_table_name}")
            logger.info(f"Truncated table '{staging_table_name}'. Deleted {count_before_truncate} records.")
        except Exception as e:
            logger.error(f"Error truncating table '{staging_table_name}': {e}")
            return

    # Write the DataFrame to BigQuery
    try:
        df.write \
            .format("bigquery") \
            .option("table", staging_table_name) \
            .mode("append") \
            .save()
        # Log the number of records loaded
        count_loaded = df.count()  
        logger.info(f"Loaded {count_loaded} records into table '{staging_table_name}'.")
    except Exception as e:
        logger.error(f"Error loading data into BigQuery: {e}")
        return

    # Unpersist the DataFrame to free up memory
    df.unpersist()


def main(yaml_file_path, output_dir, loginput_path):
    if not os.path.exists(loginput_path):
        os.makedirs(loginput_path)

    logger = setup_logging(yaml_file_path, loginput_path)

    # Generate SQL from YAML if validation passes
    generated_sql = generate_sql_from_yaml_file(yaml_file_path, logger)

    if generated_sql:
        # Check for flags in the YAML file
        with open(yaml_file_path, 'r') as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)

        if data.get('load_file', False):
            sql_file_path = os.path.join(output_dir, f"{os.path.basename(yaml_file_path).split('.')[0]}.sql")
            with open(sql_file_path, 'w') as sql_file:
                sql_file.write(generated_sql)
            logger.info(f"SQL written to file: {sql_file_path}")

        if data.get('load_table', False):
            staging_table_name = data.get('staging_table_name')  # Using staging_table_name for loading
            load_type = data.get('load_type', 'append')  # Get load_type from YAML (default to 'append')
            load_sql_to_bigquery(generated_sql, staging_table_name, load_type, logger)
            logger.info(f"SQL loaded into BigQuery table: {staging_table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate SQL from a YAML file and load to BigQuery.')
    parser.add_argument('yaml_file_path', type=str, help='Path to the input YAML file')
    parser.add_argument('output_dir', type=str, help='Directory to save the output SQL file')
    parser.add_argument('log_file_path', type=str, help='Path to the log file directory')

    args = parser.parse_args()
    main(args.yaml_file_path, args.output_dir, args.log_file_path)
