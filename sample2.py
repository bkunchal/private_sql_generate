queries = {
    "extract": [
        {
            "name": "customer_data",
            "query": """SELECT customer_id, customer_name, region FROM customer_table"""
        },
        {
            "name": "order_data",
            "query": """SELECT order_id, customer_id, order_date, order_amount FROM order_table"""
        },
        {
            "name": "product_data",
            "query": """SELECT product_id, product_name, category, price FROM product_table"""
        },
        {
            "name": "order_items",
            "query": """SELECT order_id, product_id, quantity FROM order_items_table"""
        }
    ],
    "transform": [
        {
            "name": "customer_orders",
            "query": """
                SELECT 
                    c.customer_id,
                    c.customer_name,
                    c.region,
                    o.order_id,
                    o.order_date,
                    o.order_amount
                FROM customer_data c
                JOIN order_data o ON c.customer_id = o.customer_id
            """
        },
        {
            "name": "order_details",
            "query": """
                SELECT 
                    co.customer_id,
                    co.customer_name,
                    co.region,
                    oi.order_id,
                    p.product_name,
                    p.category,
                    oi.quantity,
                    p.price,
                    (oi.quantity * p.price) AS total_price
                FROM customer_orders co
                JOIN order_items oi ON co.order_id = oi.order_id
                JOIN product_data p ON oi.product_id = p.product_id
            """
        },
        {
            "name": "final_table",
            "query": """
                SELECT 
                    customer_id,
                    customer_name,
                    region,
                    order_id,
                    product_name,
                    category,
                    quantity,
                    price,
                    total_price
                FROM order_details
            """
        }
    ],
    "load": [
        {
            "table_name": "final_table",
            "dataframe_name": "final_table",
            "load_type": "overwrite"
        }
    ]
}
