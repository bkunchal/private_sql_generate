import yaml
import os
import logging
import sys  # For accessing command-line arguments
from validation import validate_yaml  # Import the validation function
from logger import setup_logging  # Import your custom logging setup function

# Function to generate SQL from YAML
def generate_sql_from_yaml_file(file_path, logger):
    try:
        # Load the YAML content from the specified file
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
            print(f"Loaded YAML data: {data}")  # Add this line to inspect the loaded YAML data

        # Validate the loaded data against the schema, passing the logger
        if not validate_yaml(data, logger):
            raise ValueError("YAML validation failed.")

        # Start constructing the SQL query
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

        # Handle multiple UNIONs if they exist
        if 'unions' in data:
            for union in data['unions']:
                union_query = "SELECT " + ', '.join(union['select_columns']) + ' '
                union_query += f"FROM {union['table_name']} "
                
                # Add JOIN clauses in union if they exist
                if 'join_conditions' in union and union['join_conditions']:
                    for join in union['join_conditions']:
                        join_type = join.get('join_type', '').upper()
                        join_table = join.get('join_table', '')
                        join_condition = join.get('join_condition', '')
                        union_query += f"{join_type} JOIN {join_table} ON {join_condition} "
                
                # Add WHERE clause for each union if it exists
                if 'where_conditions' in union and union['where_conditions']:
                    union_query += f"WHERE {union['where_conditions']} "
                
                # Add GROUP BY clause for each union if it exists
                if 'group_by' in union and union['group_by']:
                    union_query += "GROUP BY " + ', '.join(union['group_by']) + ' '
                
                # Add ORDER BY clause for each union if it exists
                if 'order_by' in union and union['order_by']:
                    union_query += "ORDER BY " + ', '.join(union['order_by']) + ' '
                
                sql_query += f" UNION {union_query}"

        # Remove any trailing spaces and return the SQL
        sql_query = sql_query.strip()
        logger.info(f"Generated SQL: {sql_query}")
        return sql_query  # Ensure that the function returns the generated SQL
    
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
    except FileNotFoundError:
        logger.error(f"YAML file not found: {file_path}")
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return None  # Return None in case of errors

# Main execution point
if __name__ == "__main__":
    # Ensure you're capturing and printing the SQL result
    yaml_file_path = sys.argv[1]
    loginput_path = sys.argv[2]

    # Ensure the log directory exists, create it if necessary
    if not os.path.exists(loginput_path):
        os.makedirs(loginput_path)

    # Set up logging based on the YAML file name and save in loginput path
    logger = setup_logging(yaml_file_path, loginput_path)

    # Generate SQL from the YAML configuration file, passing the logger
    generated_sql = generate_sql_from_yaml_file(yaml_file_path, logger)
    
    # Print the generated SQL to the console
    if generated_sql:
        print(generated_sql)
    else:
        print("Failed to generate SQL")
