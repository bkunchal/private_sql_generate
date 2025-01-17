def transform_queries(spark, logger, config, params=None):
    """
    Executes the transformation queries from the configuration.

    :param spark: SparkSession object
    :param logger: Logger object for logging messages
    :param config: List of transformation queries
    :param params: Dictionary of dynamic parameters for substitution
    """
    for query in config:
        try:
            # Log the start of the transformation process
            logger.info(f"Executing transform query: '{query['name']}'")

            if query.get("dynamic_variable_flag"):
                # Handle dynamic parameter substitution
                if params is None:
                    logger.error(f"params needed for query: {query['name']}")
                    raise ValueError(f"params needed for query: {query['name']}")
                else:
                    final_query = query["query"].format(**params)  # Format query with params
            else:
                # No parameter substitution needed
                final_query = query["query"]

            # Execute the query and create a DataFrame
            df = spark.sql(final_query)
            view_name = query["name"]

            # Create or replace a temporary view
            df.createOrReplaceTempView(view_name)
            logger.info(f"Temp view '{view_name}' created successfully")

            # Print the DataFrame for debugging
            logger.info(f"Data for temp view '{view_name}':")
            df.show(truncate=False)  # Print the DataFrame contents

        except KeyError as e:
            logger.error(f"Missing parameter {e} for dynamic query: {query['name']}")
            raise ValueError(f"Missing parameter {e} for dynamic query: {query['name']}")
        except Exception as e:
            logger.error(f"Error executing transform query '{query['name']}': {str(e)}")
            raise
