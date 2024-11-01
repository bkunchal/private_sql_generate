from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

# Log4j logging
def get_logger(spark):
    log4j_logger = spark._jvm.org.apache.log4j
    return log4j_logger.LogManager.getLogger(__name__)

# creating or geting an existing Spark session
def create_spark_session(app_name="ETL Framework for lumi"):
    conf = SparkConf() \
        .setAppName(app_name) \
        .set("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.21.0")

    return SparkSession.builder.config(conf=conf).getOrCreate()

# loding python configuration file
def load_config(file_path):
    config = {}
    with open(file_path, "r") as file:
        exec(file.read(), config)
    return config


# Function to execute transform queries
def execute_transform_queries_parallel(spark, transform_queries, logger):
    def execute_query(query):
        try:
            logger.info(f"Executing transform query: '{query['name']}'")
            df = spark.sql(query['query'])
            view_name = query['name']
            df.createOrReplaceTempView(view_name)
            logger.info(f"Temp view created for {view_name}")
        except Exception as e:
            logger.error(f"Error executing transform query '{query['name']}': {e}")

    # Using ThreadPoolExecutor to run queries in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_query = {executor.submit(execute_query, query): query for query in transform_queries}
        for future in as_completed(future_to_query):
            query = future_to_query[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in executing query '{query['name']}': {e}")

# Main function 
def main(config_path):
    
    spark = create_spark_session()
    logger = get_logger(spark)
    config = load_config(config_path)

    if "transform" in config["queries"]:
        execute_transform_queries_parallel(spark, config["queries"]["transform"], logger)
    spark.stop()

if __name__ == "__main__":
    CONFIG_FILE_PATH = "path to config.py"
    main(CONFIG_FILE_PATH)
