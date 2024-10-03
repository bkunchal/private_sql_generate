import yaml
import logging

# Custom YAML loader to track line numbers
class LineLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)
        mapping['__line__'] = node.start_mark.line + 1  # Capture line number
        return mapping

# Define the validation function for YAML
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

    # Validate required fields and their types
    for field, field_type in required_fields.items():
        if field in data:
            if not isinstance(data[field], field_type):
                line = data.get('__line__', 'Unknown')
                logger.error(f"Validation Error: Field '{field}' must be of type {field_type.__name__}, got {type(data[field]).__name__} at line {line}.")
                return False

            # Check if the field is a string and it's not empty
            if field_type == str and not data[field].strip():
                line = data.get('__line__', 'Unknown')
                logger.error(f"Validation Error: Field '{field}' must not be an empty string at line {line}.")
                return False

            # Check if the field is a list and ensure no empty strings in the list
            if field_type == list:
                for idx, item in enumerate(data[field]):
                    # Get line number for individual list items, if present
                    line = item.get('__line__', 'Unknown') if isinstance(item, dict) else data.get('__line__', 'Unknown')
                    if not isinstance(item, str) or not item.strip():
                        logger.error(f"Validation Error: Element in '{field}' at index {idx} must be a non-empty string at line {line}.")
                        return False
        else:
            # If the field is not present but is required, log the error
            if field not in ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions', 'unions']:
                logger.error(f"Validation Error: Missing required field '{field}'.")
                return False

    # Validate join_conditions, if present
    if 'join_conditions' in data:
        for index, join in enumerate(data['join_conditions']):
            join_type = join.get('join_type', '').upper()
            join_path = f"join_conditions[{index}]"
            join_line = join.get('__line__', 'Unknown')
            
            # Allow either "CROSS" or "CROSS JOIN"
            if join_type in ["CROSS", "CROSS JOIN"]:
                if 'join_condition' in join:
                    logger.error(f"Validation Error: CROSS JOIN at {join_path} should not have join_condition at line {join_line}.")
                    return False
            else:
                # Non-CROSS JOINs must have join_table and join_condition
                if 'join_table' not in join or 'join_condition' not in join:
                    logger.error(f"Validation Error: Non-CROSS JOIN at {join_path} must contain 'join_table' and 'join_condition' at line {join_line}.")
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

                    # Validate strings in unions are not empty
                    if field_type == str and not union[field].strip():
                        logger.error(f"Validation Error at {union_path}.{field} (line {union_line}): Field '{field}' must not be an empty string.")
                        return False

                    # Validate lists in unions have no empty strings
                    if field_type == list:
                        for idx, item in enumerate(union[field]):
                            line = item.get('__line__', 'Unknown') if isinstance(item, dict) else union.get('__line__', 'Unknown')
                            if not isinstance(item, str) or not item.strip():
                                logger.error(f"Validation Error at {union_path}.{field} at index {idx}: Element must be a non-empty string at line {line}.")
                                return False

    # If all validations pass
    logger.info("YAML validation passed.")
    return True
