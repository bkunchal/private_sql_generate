import yaml
import logging

# Custom YAML loader to track line numbers for every field
class LineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        mapping['__line__'] = node.start_mark.line + 1  # Line number for this mapping
        # For every key in the mapping, track its line number
        for key_node, value_node in node.value:
            if isinstance(value_node, yaml.Node):
                mapping[f"__{key_node.value}_line__"] = value_node.start_mark.line + 1
        return mapping

# Helper method to validate strings, ensuring they are non-empty and not lists
def validate_string(value, field_name, line, logger):
    if not isinstance(value, str):
        logger.error(f"Validation Error: Field '{field_name}' must be a string, not {type(value).__name__} at line {line}.")
        return False
    if not value.strip():
        logger.error(f"Validation Error: Field '{field_name}' must be a non-empty string at line {line}.")
        return False
    return True

# Helper method to validate lists (strings or dictionaries)
def validate_list(items, field_name, parent_line, logger):
    if not isinstance(items, list) or not items:
        logger.error(f"Validation Error: Field '{field_name}' must be a non-empty list at line {parent_line}.")
        return False

    for idx, item in enumerate(items):
        if isinstance(item, str):
            line = parent_line  # Use parent's line for individual strings
            if not validate_string(item, f"{field_name}[{idx}]", line, logger):
                return False
        elif isinstance(item, dict):
            line = item.get('__line__', parent_line)  # Use item's line if available
            if field_name == 'unions':
                # Validate the fields in the union dictionaries
                if not validate_union_dict(item, f"{field_name}[{idx}]", line, logger):
                    return False
            elif field_name == 'join_conditions':
                # Validate the fields in join_conditions dictionaries
                if not validate_join_dict(item, f"{field_name}[{idx}]", line, logger):
                    return False
            else:
                logger.error(f"Validation Error: Unexpected dictionary structure in field '{field_name}' at index {idx} at line {line}.")
                return False
        else:
            line = parent_line
            logger.error(f"Validation Error: Element in '{field_name}' at index {idx} must be a non-empty string or dictionary at line {line}.")
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
        
        # Validate select_columns as a list
        if field == 'select_columns':
            if not isinstance(union[field], list):
                logger.error(f"Validation Error: Field 'select_columns' must be a list in {field_name} at line {line}.")
                return False
            if not validate_list(union[field], 'select_columns', line, logger):
                return False
        
        # Validate table_name as a string
        if field == 'table_name':
            if not isinstance(union[field], str):
                logger.error(f"Validation Error: Field 'table_name' must be a string in {field_name} at line {line}.")
                return False
            if not validate_string(union[field], 'table_name', line, logger):
                return False

    # Optional fields to validate if they exist
    optional_fields = ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions']
    
    for field in optional_fields:
        if field in union:
            # Capture the correct line number for each field if available
            field_line = union.get(f"__{field}_line__", union.get('__line__', line))

            if field == 'join_conditions':
                if not validate_list(union[field], 'join_conditions', field_line, logger):
                    return False
            elif field == 'having':
                # Validate HAVING as a string
                if not validate_string(union[field], 'having', field_line, logger):
                    return False
            elif isinstance(union[field], list):
                if not validate_list(union[field], field, field_line, logger):
                    return False
            elif isinstance(union[field], str):
                if not validate_string(union[field], field, field_line, logger):
                    return False

    return True

# Function to validate dictionaries in 'join_conditions'
def validate_join_dict(join, field_name, line, logger):
    # Required fields for each join condition
    required_fields = ['join_type', 'join_table', 'join_condition']
    
    for field in required_fields:
        if field not in join:
            if field == 'join_condition' and join.get('join_type', '').upper() in ["CROSS", "CROSS JOIN"]:
                # CROSS JOIN doesn't require a join_condition
                continue
            logger.error(f"Validation Error: Missing required field '{field}' in {field_name} at line {line}.")
            return False

        if field == 'join_type':
            if join[field].upper() in ["CROSS", "CROSS JOIN"]:
                # CROSS JOIN should not have a join_condition
                if 'join_condition' in join:
                    logger.error(f"Validation Error: CROSS JOIN in {field_name} should not have 'join_condition' at line {line}.")
                    return False
            if not validate_string(join[field], 'join_type', line, logger):
                return False

        if field in ['join_table', 'join_condition']:
            if not validate_string(join[field], field, line, logger):
                return False

    return True

# Main validation function for YAML
def validate_yaml(data, logger):
    # Ensure the data is a dictionary
    if not isinstance(data, dict):
        line = data.get('__line__', 'Unknown')
        logger.error(f"Validation Error at root (line {line}): YAML data should be a dictionary.")
        return False

    # Required fields with specific types
    required_fields = {
        'select_columns': list,
        'table_name': str,
        'join_conditions': list,  # Optional
        'where_conditions': str,  # Optional
        'group_by': list,  # Optional
        'having': str,  # Optional
        'order_by': list,  # Optional
        'unions': list  # Optional
    }

    # Validate required fields and their types, and ensure no empty strings or empty lists
    for field, field_type in required_fields.items():
        if field in data:
            line = data.get(f"__{field}_line__", data.get('__line__', 'Unknown'))

            if not isinstance(data[field], field_type):
                logger.error(f"Validation Error: Field '{field}' must be of type {field_type.__name__}, got {type(data[field]).__name__} at line {line}.")
                return False

            # Validate string fields
            if field_type == str:
                if not validate_string(data[field], field, line, logger):
                    return False

            # Validate list fields
            if field_type == list:
                if not validate_list(data[field], field, line, logger):
                    return False
        else:
            # If the field is not present but is required, log the error
            if field not in ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions', 'unions']:
                logger.error(f"Validation Error: Missing required field '{field}' at line {line}.")
                return False

    # Validate unions, if present
    if 'unions' in data:
        for index, union in enumerate(data['unions']):
            union_path = f"unions[{index}]"
            union_line = union.get('__line__', 'Unknown')

            # Validate the dictionary structure of the union
            if not validate_union_dict(union, union_path, union_line, logger):
                return False

    logger.info("YAML validation passed.")
    return True
