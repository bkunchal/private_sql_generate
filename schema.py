
schema = {
    'select_columns': {
        'type': 'list',
        'schema': {
            'type': 'string',
        },
        'required': True,  # select_columns are required
    },
    'table_name': {
        'type': 'string',
        'required': True,  # table_name is required
    },
    'join_conditions': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'join_type': {'type': 'string', 'required': True},
                'join_table': {'type': 'string', 'required': True},
                'join_condition': {'type': 'string', 'required': True},
            },
        },
        'required': False,  # join_conditions are optional
    },
    'where_conditions': {
        'type': 'string',
        'required': False,  # where_conditions are optional
    },
    'group_by': {
        'type': 'list',
        'schema': {
            'type': 'string',
        },
        'required': False,  # group_by is optional
    },
    'order_by': {
        'type': 'list',
        'schema': {
            'type': 'string',
        },
        'required': False,  # order_by is optional
    },
}
