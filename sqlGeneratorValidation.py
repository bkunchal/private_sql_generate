import yaml
import logging

# Custom YAML loader to track line numbers for every field
class LineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        mapping['__line__'] = node.start_mark.line + 1 
        for key_node, value_node in node.value:
            if isinstance(value_node, yaml.Node):
                mapping[f"__{key_node.value}_line__"] = value_node.start_mark.line + 1
        return mapping

    def construct_sequence(self, node, deep=False):
        sequence = super().construct_sequence(node, deep=deep)
        for idx, item in enumerate(node.value):
            if isinstance(item, yaml.ScalarNode):
                sequence[idx] = {
                    'value': item.value,
                    '__line__': item.start_mark.line + 1  
                }
        return sequence

# Helper method to validate strings, ensuring they are non-empty
def validate_string(value, field_name, line, logger):
    if not isinstance(value, str):
        logger.error(f"Validation Error: Field '{field_name}' must be a string, not {type(value).__name__} at line {line}.")
        return False
    if not value.strip():  
        logger.error(f"Validation Error: Field '{field_name}' must be a non-empty string at line {line}.")
        return False
    return True

def validate_list(items, field_name, parent_line, logger):
    if not isinstance(items, list) or not items:
        logger.error(f"Validation Error: Field '{field_name}' must be a non-empty list at line {parent_line}.")
        return False

    for idx, item in enumerate(items):
        line = parent_line  
        if isinstance(item, dict) and 'value' in item and '__line__' in item:
            line = item['__line__']
            item_value = item['value']
        elif isinstance(item, dict):
            item_value = item
        else:
            item_value = item

        if isinstance(item_value, str):
            if not validate_string(item_value, f"{field_name}[{idx}]", line, logger):
                return False
        elif isinstance(item_value, dict):
            if field_name == 'unions':
                if not validate_union_dict(item_value, f"{field_name}[{idx}]", line, logger):
                    return False
            elif field_name == 'join_conditions':
                if not validate_join_dict(item_value, f"{field_name}[{idx}]", line, logger):
                    return False
            else:
                logger.error(f"Validation Error: Unexpected dictionary structure in field '{field_name}' at index {idx} at line {line}.")
                return False
        else:
            logger.error(f"Validation Error: Element in '{field_name}' at index {idx} must be a non-empty string or dictionary at line {line}.")
            return False
    return True


# Function to validate dictionaries in 'join_conditions'

def validate_join_dict(join, field_name, line, logger):
    # Required fields for each join condition
    required_fields = ['join_type', 'join_table']

    for field in required_fields:
        field_line = join.get(f"__{field}_line__", line)  
        if field not in join or not join[field].strip():
            logger.error(f"Validation Error: Field '{field}' in {field_name} must be a non-empty string at line {field_line}.")
            return False

    # If it's a CROSS JOIN, it should not have a join_condition
    if join['join_type'].upper() in ["CROSS", "CROSS JOIN"]:
        if 'join_condition' in join:
            logger.error(f"Validation Error: CROSS JOIN in {field_name} should not have 'join_condition' at line {line}.")
            return False
    else:
        # For non-CROSS JOINs, ensure join_condition is present and non-empty
        if 'join_condition' not in join or not join['join_condition'].strip():
            field_line = join.get(f"__join_condition_line__", line)
            logger.error(f"Validation Error: Missing or empty 'join_condition' in {field_name} at line {field_line}.")
            return False

    return True


# Function to validate dictionaries in 'unions'
def validate_union_dict(union, field_name, line, logger):
    # Required fields in union
    required_union_fields = ['select_columns', 'table_name']

    for field in required_union_fields:
        if field not in union:
            logger.error(f"Validation Error: Missing required field '{field}' in {field_name} at line {line}.")
            return False

        field_line = union.get(f"__{field}_line__", line)

        if field == 'select_columns':
            if not isinstance(union[field], list):
                logger.error(f"Validation Error: Field 'select_columns' must be a list in {field_name} at line {field_line}.")
                return False
            if not validate_list(union[field], 'select_columns', field_line, logger):
                return False

        elif field == 'table_name':
            if not validate_string(union[field], 'table_name', field_line, logger):
                return False

    # Optional fields to validate if they exist
    optional_fields = ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions']

    for field in optional_fields:
        if field in union:
            field_line = union.get(f"__{field}_line__", union.get('__line__', line))

            if field == 'where_conditions':
                # Ensure where_conditions is a string
                if not isinstance(union[field], str):
                    logger.error(f"Validation Error: Field 'where_conditions' must be a string in {field_name} at line {field_line}.")
                    return False
                if not validate_string(union[field], 'where_conditions', field_line, logger):
                    return False

            elif field == 'order_by':
                # Ensure order_by is a list
                if not isinstance(union[field], list):
                    logger.error(f"Validation Error: Field 'order_by' must be a list in {field_name} at line {field_line}.")
                    return False
                if not validate_list(union[field], 'order_by', field_line, logger):
                    return False

            elif field == 'group_by':
                # Ensure group_by is a list
                if not isinstance(union[field], list):
                    logger.error(f"Validation Error: Field 'group_by' must be a list in {field_name} at line {field_line}.")
                    return False
                if not validate_list(union[field], 'group_by', field_line, logger):
                    return False

            elif field == 'join_conditions':
                if not validate_list(union[field], 'join_conditions', field_line, logger):
                    return False
            elif field == 'having':
                if not validate_string(union[field], 'having', field_line, logger):
                    return False
            elif isinstance(union[field], list):
                if not validate_list(union[field], field, field_line, logger):
                    return False
            elif isinstance(union[field], str):
                if not validate_string(union[field], field, field_line, logger):
                    return False

    return True



# Main validation function for YAML
def validate_yaml(data, logger):
    if not isinstance(data, dict):
        line = data.get('__line__', 'Unknown')
        logger.error(f"Validation Error at root (line {line}): YAML data should be a dictionary.")
        return False

    required_fields = {
        'select_columns': list,
        'table_name': str,
        'join_conditions': list,
        'where_conditions': str,  
        'group_by': list,
        'having': str,
        'order_by': list,
        'unions': list
    }

    for field, field_type in required_fields.items():
        if field in data:
            line = data.get(f"__{field}_line__", data.get('__line__', 'Unknown'))

            if not isinstance(data[field], field_type):
                logger.error(f"Validation Error: Field '{field}' must be of type {field_type.__name__}, got {type(data[field]).__name__} at line {line}.")
                return False

            # Validate strings and ensure they are non-empty
            if field_type == str:
                if not validate_string(data[field], field, line, logger):
                    return False

            # Validate lists
            if field_type == list:
                if not validate_list(data[field], field, line, logger):
                    return False
        else:
            # For optional fields, do not log an error if they are missing
            if field not in ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions', 'unions']:
                logger.error(f"Validation Error: Missing required field '{field}' at line {line}.")
                return False

    logger.info("YAML validation passed.")
    return True