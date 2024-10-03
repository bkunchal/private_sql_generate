import yaml
import logging
import os
import sys
from validation import validate_yaml, LineLoader  # Import the validation function and LineLoader
from logger import setup_logging  # Custom logging setup (assuming you have it)

# Function to generate SQL from YAML
def generate_sql_from_yaml_file(file_path, logger):
    try:
        # Load the YAML content from the specified file using LineLoader for line tracking
        with open(file_path, 'r') as file:
            data = yaml.load(file, Loader=LineLoader)  # Use LineLoader to track line numbers
            print(f"Loaded YAML data: {data}")

        # Validate the loaded YAML
        if not validate_yaml(data, logger):
            raise ValueError("YAML validation failed.")

        # Start constructing the SQL query after validation passes
        sql_query = "SELECT "

        # Add SELECT columns
        sql_query += ', '.join(data['select_columns']) + ' '
        
        # Add FROM clause
        sql_query += f"FROM {data['table_name']} "
        
        # Add JOIN clauses if they exist
        if 'join_conditions' in data and data['join_conditions']:
            for join in data['join_conditions']:
                join_type = join.get('join_type', '').upper()
                join_table = join.get('join_table', '')

                # Handle CROSS JOIN
                if join_type in ["CROSS", "CROSS JOIN"]:
                    sql_query += f"{join_type} {join_table} "
                else:
                    join_condition = join.get('join_condition', '')
                    sql_query += f"{join_type} JOIN {join_table} ON {join_condition} "

        # Add WHERE clause if it exists
        if 'where_conditions' in data and data['where_conditions']:
            sql_query += f"WHERE {data['where_conditions']} "

        # Add GROUP BY clause if it exists
        if 'group_by' in data and data['group_by']:
            sql_query += "GROUP BY " + ', '.join(data['group_by']) + ' '
        
        # Add HAVING clause if it exists
        if 'having' in data and data['having']:
            sql_query += f"HAVING {data['having']} "

        # Add ORDER BY clause if it exists
        if 'order_by' in data and data['order_by']:
            sql_query += "ORDER BY " + ', '.join(data['order_by']) + ' '

        # Handle UNIONs if they exist
        if 'unions' in data:
            for union in data['unions']:
                union_query = "SELECT " + ', '.join(union['select_columns']) + ' '
                union_query += f"FROM {union['table_name']} "
                
                if 'join_conditions' in union and union['join_conditions']:
                    for join in union['join_conditions']:
                        join_type = join.get('join_type', '').upper()
                        join_table = join.get('join_table', '')

                        # Handle CROSS JOIN in union
                        if join_type in ["CROSS", "CROSS JOIN"]:
                            union_query += f"{join_type} {join_table} "
                        else:
                            join_condition = join.get('join_condition', '')
                            union_query += f"{join_type} JOIN {join_table} ON {join_condition} "

                if 'where_conditions' in union and union['where_conditions']:
                    union_query += f"WHERE {union['where_conditions']} "

                if 'group_by' in union and union['group_by']:
                    union_query += "GROUP BY " + ', '.join(union['group_by']) + ' '

                if 'having' in union and union['having']:
                    union_query += f"HAVING {union['having']} "

                if 'order_by' in union and union['order_by']:
                    union_query += "ORDER BY " + ', '.join(union['order_by']) + ' '

                sql_query += f" UNION {union_query}"

        # Clean up SQL and log the result
        sql_query = sql_query.strip()
        logger.info(f"Generated SQL: {sql_query}")
        return sql_query

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return None
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return None

# Main execution
if __name__ == "__main__":
    yaml_file_path = "sample.yaml"  # Replace with your actual YAML file path
    loginput_path = "logs"  # Replace with your log directory

    # Ensure the log directory exists
    if not os.path.exists(loginput_path):
        os.makedirs(loginput_path)

    # Set up logging
    logger = setup_logging(yaml_file_path, loginput_path)

    # Generate SQL from YAML if validation passes
    generated_sql = generate_sql_from_yaml_file(yaml_file_path, logger)

    if generated_sql:
        print("Generated SQL:\n", generated_sql)
    else:
        print("Failed to generate SQL.")
