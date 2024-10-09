import argparse  
import yaml
import logging
import os
import sys
from validation import validate_yaml, LineLoader
from logger import setup_logging

# Helper function to handle JOINs
def generate_join_clause(joins):
    join_clause = ""
    for join in joins:
        join_type = join.get('join_type', '').upper()
        join_table = join.get('join_table', '')
        if join_type == "CROSS" or join_type == "CROSS JOIN":
            join_clause += f"CROSS JOIN {join_table} "
        else:
            join_condition = join.get('join_condition', '')
            join_clause += f"{join_type} JOIN {join_table} ON {join_condition} "
    
    return join_clause


# Helper function to extract values from lists (e.g., select_columns, group_by, order_by)
def extract_list_values(data_list):
    return [
        item['value'] if isinstance(item, dict) and 'value' in item else item
        for item in data_list
    ]

# Helper function to handle common SQL clauses (WHERE, GROUP BY, HAVING, ORDER BY)
def generate_sql_clauses(data):
    sql_clauses = ""

    # Add WHERE clause
    if 'where_conditions' in data and data['where_conditions']:
        sql_clauses += f"WHERE {data['where_conditions']} "

    # Add GROUP BY clause
    if 'group_by' in data and data['group_by']:
        group_by_columns = extract_list_values(data['group_by'])
        sql_clauses += "GROUP BY " + ', '.join(group_by_columns) + ' '

    # Add HAVING clause
    if 'having' in data and data['having']:
        sql_clauses += f"HAVING {data['having']} "

    # Add ORDER BY clause
    if 'order_by' in data and data['order_by']:
        order_by_columns = extract_list_values(data['order_by'])
        sql_clauses += "ORDER BY " + ', '.join(order_by_columns) + ' '

    return sql_clauses

# Function to handle UNIONs
def generate_union_clauses(unions):
    union_sql = ""
    for union in unions:
        select_columns = extract_list_values(union['select_columns'])
        union_query = "SELECT " + ', '.join(select_columns) + ' '
        union_query += f"FROM {union['table_name']} "

        # Add JOIN clauses in union
        if 'join_conditions' in union and union['join_conditions']:
            union_query += generate_join_clause(union['join_conditions'])

        # Add other SQL clauses (WHERE, GROUP BY, HAVING, ORDER BY)
        union_query += generate_sql_clauses(union)
        union_sql += f" UNION {union_query}"

    return union_sql

# Function to generate SQL from YAML
def generate_sql_from_yaml_file(file_path, output_dir, logger):
    try:
        with open(file_path, 'r') as file:
            data = yaml.load(file, Loader=LineLoader)
            print(f"Loaded YAML data: {data}")

        # Validate the loaded YAML
        if not validate_yaml(data, logger):
            raise ValueError("YAML validation failed.")

        # Start constructing the SQL query after validation passes
        select_columns = extract_list_values(data['select_columns'])
        sql_query = "SELECT " + ', '.join(select_columns) + ' '
        sql_query += f"FROM {data['table_name']} "

        # Add JOIN clauses
        if 'join_conditions' in data and data['join_conditions']:
            sql_query += generate_join_clause(data['join_conditions'])

        # Add WHERE, GROUP BY, HAVING, ORDER BY clauses
        sql_query += generate_sql_clauses(data)

        # Handle UNIONs if they exist
        if 'unions' in data:
            sql_query += generate_union_clauses(data['unions'])

        # Clean up SQL and log the result
        sql_query = sql_query.strip()
        logger.info(f"Generated SQL: {sql_query}")

        # Create the output SQL file path using the base name of the YAML file
        base_name = os.path.splitext(os.path.basename(file_path))[0]  # Get base name without extension
        sql_file_path = os.path.join(output_dir, f"{base_name}.sql")  # Combine with output directory

        # Write the generated SQL to the specified .sql file
        with open(sql_file_path, 'w') as sql_file:
            sql_file.write(sql_query)
        logger.info(f"SQL written to {sql_file_path}")

        return sql_query

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return None
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return None



# Main execution
if __name__ == "__main__": 
    # Set up argument parsing
    # parser = argparse.ArgumentParser(description='Generate SQL from a YAML file.')
    # parser.add_argument('yaml_file_path', type=str, help='Path to the input YAML file')
    # parser.add_argument('output_dir', type=str, help='Directory to save the output SQL file')
    # parser.add_argument('log_file_path', type=str, help='Path to the log file directory')

    # # Parse the command-line arguments
    # args = parser.parse_args()
    # yaml_file_path = args.yaml_file_path
    # output_dir = args.output_dir  # Get output directory from arguments
    # log_file_path = args.log_file_path 
    yaml_file_path = "C:\\Users\\balaji kunchala\\Documents\\sample.yaml"
    output_dir = "C:\\Users\\balaji kunchala\\Documents\\sql_generator\\sql"

    loginput_path = "C:\\Users\\balaji kunchala\\Documents\\sql_generator\\logs"

    if not os.path.exists(loginput_path):
        os.makedirs(loginput_path)

    logger = setup_logging(yaml_file_path, loginput_path)

    # Generate SQL from YAML if validation passes
    generated_sql = generate_sql_from_yaml_file(yaml_file_path, output_dir, logger)  # Pass output_dir here

    if generated_sql:
        print("Generated SQL:\n", generated_sql)
    else:
        print("Failed to generate SQL.")