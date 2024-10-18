import argparse
import yaml
import logging
import os
from sql_generator import generate_sql_from_yaml_file
from logger import setup_logging
from pyspark.sql import SparkSession
from datetime import datetime


def create_spark_session():
    return SparkSession.builder \
        .appName("Load SQL to BigQuery") \
        .getOrCreate()


def load_sql_to_bigquery(sql_query, stagging_table_name, load_type):
    spark = create_spark_session()

    # Check if the table exists
    table_exists = spark._jvm.com.google.cloud.bigquery.BigQuery.createOrGetDataset().tableExists(stagging_table_name)

    # If the table doesn't exist, create it
    if not table_exists:
        # Extract column names from the SQL query
        columns = [col.strip() for col in sql_query.split("SELECT")[1].split("FROM")[0].split(",")]
        columns_with_types = ', '.join(
            [f"{col.split(' ')[-1]} STRING" for col in columns])  

        # Adding load_ts column to the create table statement
        create_table_query = f"CREATE TABLE {stagging_table_name} ({columns_with_types}, load_ts TIMESTAMP)"
        spark.sql(create_table_query)

    # Truncate the table if load_type is 'truncate'
    if load_type == 'truncate':
        spark.sql(f"TRUNCATE TABLE {stagging_table_name}")

    # Load the data into a DataFrame, adding the load_ts column
    df = spark.sql(sql_query)

    # Add the load_ts column with the current timestamp
    df = df.withColumn("load_ts", datetime.now())

    # Write the DataFrame to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", stagging_table_name) \
        .mode("append") \
        .save()


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
            stagging_table_name = data.get('stagging_table_name')  # Using stagging_table_name for loading
            load_type = data.get('load_type', 'append')  # Get load_type from YAML (default to 'append')
            load_sql_to_bigquery(generated_sql, stagging_table_name, load_type)
            logger.info(f"SQL loaded into BigQuery table: {stagging_table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate SQL from a YAML file and load to BigQuery.')
    parser.add_argument('yaml_file_path', type=str, help='Path to the input YAML file')
    parser.add_argument('output_dir', type=str, help='Directory to save the output SQL file')
    parser.add_argument('log_file_path', type=str, help='Path to the log file directory')

    args = parser.parse_args()
    main(args.yaml_file_path, args.output_dir, args.log_file_path)
