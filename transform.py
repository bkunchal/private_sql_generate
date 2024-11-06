from config import get_logger, create_spark_session, load_config
            
def execute_transform_queries(spark, config, logger):
    if "transform" in config["queries"]:
        transform_queries = config["queries"]["transform"]
        for query in transform_queries:
            try:
                logger.info(f"Executing transform query: '{query['name']}'")
                df = spark.sql(query['query'])
                view_name = query['name']
                df.createOrReplaceTempView(view_name)
                logger.info(f"Temp view created for {view_name}")
            except Exception as e:
                logger.error(f"Error executing transform query '{query['name']}': {e}")
    else:
        logger.info("No transform queries found in the configuration. Skipping transformation phase.")
