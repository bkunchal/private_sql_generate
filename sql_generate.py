import yaml
import logging
import os
from cerberus import Validator
from schema import schema  # Ensure you import your schema here

# Function to set up logging dynamically based on the config file
def setup_logging(config_file, log_directory):
    # Extract the config filename without the extension
    config_filename = os.path.splitext(os.path.basename(config_file))[0]
    
    # Define the log file name and path
    log_file_path = os.path.join(log_directory, f"{config_filename}.log")

    # Set up logging
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Logging started.")
    logging.info(f"Log file: {log_file_path}")

def validate_yaml(data):
    v = Validator(schema)
    if not v.validate(data):
        logging.error(f"YAML validation errors: {v.errors}")
        return False
    return True

def generate_sql_from_yaml_file(file_path):
    try:
        # Load the YAML content from the specified file
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)

        # Validate the loaded data against the schema
        if not validate_yaml(data):
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
        
        # Add ORDER BY clause if it exists
        if 'order_by' in data and data['order_by']:
            sql_query += "ORDER BY " + ', '.join(data['order_by']) + ' '

        # Remove any trailing spaces and return the SQL
        sql_query = sql_query.strip()
        logging.info(f"Generated SQL: {sql_query}")
        return sql_query
    
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML: {e}")
    except FileNotFoundError:
        logging.error(f"YAML file not found: {file_path}")
    except Exception as e:
        logging.error(f"Error generating SQL: {e}")

# Main execution point
if __name__ == "__main__":
    # Replace sys.argv with hardcoded paths
    yaml_file_path = 'C:/Users/balaji kunchala/Documents/sample.yaml'  # Set your YAML file path here
    loginput_path = 'C:/Users/balaji kunchala/Documents/sql_generator/logs'  # Set your log directory path here

    # Ensure the log directory exists, create it if necessary
    if not os.path.exists(loginput_path):
        os.makedirs(loginput_path)

    # Set up logging based on the YAML file name and save in loginput path
    setup_logging(yaml_file_path, loginput_path)

    # Generate SQL from the YAML configuration file
    generated_sql = generate_sql_from_yaml_file(yaml_file_path)
    print(generated_sql)
