from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Updated connection ID to match what you'll define in Airflow
SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    "snowflake_sales_analytics",
    description="Fetch and analyze data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    tags=["simple_snowflake_batch"],
    schedule=None,
    catchup=False,
) as dag:
    # Task to query customer data
    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        sql="SELECT * FROM sales_analytics.retail.customers;",
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to query product data
    fetch_products = SQLExecuteQueryOperator(
        task_id="fetch_products",
        sql="SELECT * FROM sales_analytics.retail.products;",
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to get order summary
    order_summary = SQLExecuteQueryOperator(
        task_id="order_summary",
        sql="""
        SELECT 
            o.order_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            o.order_date,
            o.status,
            o.total_amount,
            COUNT(oi.product_id) AS item_count
        FROM sales_analytics.retail.orders o
        JOIN sales_analytics.retail.customers c ON o.customer_id = c.customer_id
        JOIN sales_analytics.retail.order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, customer_name, o.order_date, o.status, o.total_amount
        ORDER BY o.order_date DESC;
        """,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to analyze sales by category
    sales_by_category = SQLExecuteQueryOperator(
        task_id="sales_by_category",
        sql="""
        SELECT 
            p.category,
            SUM(oi.quantity) AS total_quantity_sold,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM sales_analytics.retail.order_items oi
        JOIN sales_analytics.retail.products p ON oi.product_id = p.product_id
        JOIN sales_analytics.retail.orders o ON oi.order_id = o.order_id
        WHERE o.status = 'completed' OR o.status = 'shipped'
        GROUP BY p.category
        ORDER BY total_revenue DESC;
        """,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Define task dependencies
    fetch_customers >> fetch_products >> order_summary >> sales_by_category
