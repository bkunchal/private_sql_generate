from spark_utils import initialize_spark
from bigquery_utils import load_data_bigquery, write_to_bigquery

def main():
    spark = initialize_spark("gcp_corporate_sales_analysis")

    # Step 1: Extract Customer Orders
    customer_orders_query = """
    SELECT 
        c.customer_id,
        c.customer_name,
        c.region,
        o.order_id,
        o.order_date,
        o.order_amount
    FROM sales.customers c
    INNER JOIN sales.orders o ON c.customer_id = o.customer_id
    WHERE o.order_date >= '2023-01-01'
    """

    customer_orders_df = load_data_bigquery(spark, customer_orders_query)
    temp_customer_orders = "temp.customer_orders"
    write_to_bigquery(customer_orders_df, temp_customer_orders)

    # Step 2: Enrich Orders with Products & Quantities
    enriched_orders_query = """
    SELECT 
        co.customer_id,
        co.customer_name,
        co.region,
        co.order_id,
        co.order_date,
        co.order_amount,
        oi.product_id,
        p.product_name,
        p.category,
        oi.quantity,
        oi.quantity * p.price AS total_price
    FROM temp.customer_orders co
    INNER JOIN sales.order_items oi ON co.order_id = oi.order_id
    INNER JOIN sales.products p ON oi.product_id = p.product_id
    """

    enriched_orders_df = load_data_bigquery(spark, enriched_orders_query)
    temp_enriched_orders = "temp.enriched_orders"
    write_to_bigquery(enriched_orders_df, temp_enriched_orders)

    # Step 3: Aggregate Regional Sales Data
    regional_sales_query = """
    SELECT 
        eo.region,
        SUM(eo.order_amount) AS total_sales,
        COUNT(DISTINCT eo.order_id) AS order_count,
        COUNT(DISTINCT eo.customer_id) AS unique_customers
    FROM temp.enriched_orders eo
    WHERE eo.order_amount > 500
    GROUP BY eo.region
    HAVING SUM(eo.order_amount) > 2000
    """

    regional_sales_df = load_data_bigquery(spark, regional_sales_query)
    temp_regional_sales = "temp.regional_sales"
    write_to_bigquery(regional_sales_df, temp_regional_sales)

    # Step 4: Compute Product Sales Summary
    product_sales_query = """
    SELECT 
        eo.category,
        COUNT(DISTINCT eo.order_id) AS order_count,
        SUM(eo.quantity) AS total_quantity,
        SUM(eo.total_price) AS total_revenue
    FROM temp.enriched_orders eo
    GROUP BY eo.category
    ORDER BY total_revenue DESC
    """

    product_sales_df = load_data_bigquery(spark, product_sales_query)
    temp_product_sales = "temp.product_sales"
    write_to_bigquery(product_sales_df, temp_product_sales)

if __name__ == "__main__":
    main()
