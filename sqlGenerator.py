import yaml
import sqlparse
import sqlglot
import logging
import os
from sqlGeneratorValidation import validate_yaml, LineLoader  

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

# Helper function to extract values from lists (select_columns, group_by, order_by)
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
def generate_sql_from_yaml_file(file_path, logger):
    try:
        with open(file_path, 'r') as file:
            data = yaml.load(file, Loader=LineLoader)
            logger.info(f"Loaded YAML data: {data}")

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

        try:
            sqlglot.parse(sql_query)  
            logger.info("SQL is valid.")
        except Exception as e:
            logger.error(f"Generated SQL is invalid: {e}")
            raise ValueError(f"Generated SQL validation failed: {str(e)}")

        # Format the SQL query using sqlparse
        formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')

        return formatted_sql  

    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return None
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return None
