import yaml
import logging

# Custom YAML loader to track line numbers
class LineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        mapping['__line__'] = node.start_mark.line + 1  # Capture line number
        return mapping

# Helper method to validate strings
def validate_string(value, field_name, line, logger):
    if not isinstance(value, str) or not value.strip():
        logger.error(f"Validation Error: Field '{field_name}' must be a non-empty string at line {line}.")
        return False
    return True

# Helper method to validate lists (strings or dictionaries)
def validate_list(items, field_name, logger):
    for idx, item in enumerate(items):
        if isinstance(item, str):
            # We assume the line number comes from the parent list since individual strings don't have line info
            line = 'Unknown'  # Update with the correct parent line number if available
            if not validate_string(item, f"{field_name}[{idx}]", line, logger):
                return False
        elif isinstance(item, dict):
            line = item.get('__line__', 'Unknown')  # Correctly use .get() for dicts only
            if field_name == 'join_conditions':
                # Handle CROSS JOIN or regular join validation in lists of dicts
                join_type = item.get('join_type', '').upper()
                if join_type in ["CROSS", "CROSS JOIN"]:
                    if 'join_condition' in item:
                        logger.error(f"Validation Error: CROSS JOIN at {field_name}[{idx}] should not have 'join_condition' at line {line}.")
                        return False
                    if not validate_string(item.get('join_table', ''), 'join_table', line, logger):
                        return False
                else:
                    if not validate_string(item.get('join_table', ''), 'join_table', line, logger):
                        return False
                    if not validate_string(item.get('join_condition', ''), 'join_condition', line, logger):
                        return False
            else:
                logger.error(f"Validation Error: Unexpected dictionary structure in field '{field_name}' at index {idx}.")
                return False
        else:
            line = 'Unknown'  # Default to a parent line or specify 'Unknown'
            logger.error(f"Validation Error: Element in '{field_name}' at index {idx} must be a dictionary or a non-empty string at line {line}.")
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
        'join_conditions': list,  # Optional, but should be a list if present
        'where_conditions': str,  # Optional, but should be a string if present
        'group_by': list,  # Optional, but should be a list if present
        'having': str,  # Optional, but should be a string if present
        'order_by': list,  # Optional, but should be a list if present
        'unions': list  # Optional, but should be a list of union dictionaries
    }

    # Validate required fields and their types, and ensure no empty strings
    for field, field_type in required_fields.items():
        if field in data:
            if not isinstance(data[field], field_type):
                line = data.get('__line__', 'Unknown')
                logger.error(f"Validation Error: Field '{field}' must be of type {field_type.__name__}, got {type(data[field]).__name__} at line {line}.")
                return False

            # Validate string fields
            if field_type == str:
                line = data.get('__line__', 'Unknown')
                if not validate_string(data[field], field, line, logger):
                    return False

            # Validate list fields
            if field_type == list:
                if not validate_list(data[field], field, logger):
                    return False
        else:
            # If the field is not present but is required, log the error
            if field not in ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions', 'unions']:
                logger.error(f"Validation Error: Missing required field '{field}'.")
                return False

    # Validate unions, if present
    if 'unions' in data:
        for index, union in enumerate(data['unions']):
            union_path = f"unions[{index}]"
            union_line = union.get('__line__', 'Unknown')

            # Required fields in union
            union_required_fields = {
                'select_columns': list,
                'table_name': str,
                'where_conditions': str,  # Optional, but should be a string if present
                'group_by': list,  # Optional
                'having': str,  # Optional
                'order_by': list,  # Optional
                'join_conditions': list  # Optional, but should be a list if present
            }

            for field, field_type in union_required_fields.items():
                if field in union:
                    if not isinstance(union[field], field_type):
                        logger.error(f"Validation Error at {union_path}.{field} (line {union_line}): Field '{field}' must be of type {field_type.__name__}, got {type(union[field]).__name__}.")
                        return False

                    # Validate string fields in union
                    if field_type == str:
                        if not validate_string(union[field], f"{union_path}.{field}", union_line, logger):
                            return False

                    # Validate list fields in union
                    if field_type == list:
                        if not validate_list(union[field], f"{union_path}.{field}", logger):
                            return False

    # If all validations pass
    logger.info("YAML validation passed.")
    return True