from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Define datasets for data-aware scheduling
product_dataset = Dataset("snowflake://sales_analytics.retail.products")
order_summary_dataset = Dataset("snowflake://sales_analytics.retail.order_summary")

with DAG(
    "snowflake_dag_3",
    description="Generate order summary from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=[product_dataset],  # Run when product_dataset is updated
    catchup=False,
) as dag:
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
        outlets=[order_summary_dataset],
    )
