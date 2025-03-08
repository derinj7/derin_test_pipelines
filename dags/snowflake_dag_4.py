from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Define dataset for data-aware scheduling
order_summary_dataset = Dataset('snowflake://sales_analytics.retail.order_summary')

with DAG(
    "snowflake_dag_4",
    description="Analyze sales by category from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=[order_summary_dataset],  # Run when order_summary_dataset is updated
    catchup=False,
) as dag:

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