import logging

class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass

# Define the custom validation function for YAML
def validate_yaml(data, logger):
    # Ensure the data is a dictionary
    if not isinstance(data, dict):
        logger.error("Validation Error: YAML data should be a dictionary.")
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
                logger.error(f"Validation Error: Field '{field}' must be of type {field_type.__name__}.")
                return False
        else:
            if field not in ['where_conditions', 'group_by', 'having', 'order_by', 'join_conditions', 'unions']:
                logger.error(f"Validation Error: Missing required field '{field}'.")
                return False

    # Validate fields within join conditions if present
    if 'join_conditions' in data:
        for index, join in enumerate(data['join_conditions']):
            if not isinstance(join, dict):
                logger.error(f"Validation Error: Join condition at index {index} must be a dictionary.")
                return False

            join_type = join.get('join_type', '').upper()

            # CROSS JOIN should not have a join_condition
            if join_type == "CROSS JOIN" and 'join_condition' in join:
                logger.error(f"Validation Error: CROSS JOIN at index {index} should not have a join_condition.")
                return False

            # Non-CROSS JOINs should have join_table and join_condition
            if join_type != "CROSS JOIN":
                if 'join_table' not in join or 'join_condition' not in join:
                    logger.error(f"Validation Error: Join condition at index {index} must contain 'join_table' and 'join_condition' for non-CROSS JOIN.")
                    return False

    # Validate unions if present
    if 'unions' in data:
        for union_index, union in enumerate(data['unions']):
            union_required_fields = {
                'select_columns': list,
                'table_name': str,
                'where_conditions': str,  # Optional, but must be a string if present
                'group_by': list,  # Optional
                'having': str,  # Optional
                'order_by': list,  # Optional
                'join_conditions': list  # Optional, but should be a list if present
            }
            for field, field_type in union_required_fields.items():
                if field in union:
                    if not isinstance(union[field], field_type):
                        logger.error(f"Validation Error: Union at index {union_index} field '{field}' must be of type {field_type.__name__}.")
                        return False
                else:
                    if field in ['select_columns', 'table_name']:
                        logger.error(f"Validation Error: Missing required union field '{field}' in union at index {union_index}.")
                        return False

            # Validate join_conditions in unions
            if 'join_conditions' in union:
                for join_index, join in enumerate(union['join_conditions']):
                    join_type = join.get('join_type', '').upper()

                    # CROSS JOIN should not have a join_condition in unions
                    if join_type == "CROSS JOIN" and 'join_condition' in join:
                        logger.error(f"Validation Error: Union CROSS JOIN at index {union_index}, join {join_index} should not have a join_condition.")
                        return False

                    # Non-CROSS JOINs should have join_table and join_condition in unions
                    if join_type != "CROSS JOIN":
                        if 'join_table' not in join or 'join_condition' not in join:
                            logger.error(f"Validation Error: Union join condition at index {union_index}, join {join_index} must contain 'join_table' and 'join_condition' for non-CROSS JOIN.")
                            return False

    # If all validations pass, return True
    logger.info("YAML validation passed.")
    return True
