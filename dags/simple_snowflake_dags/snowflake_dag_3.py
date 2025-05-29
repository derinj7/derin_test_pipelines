from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "snowflake_default"

# Define datasets for data-aware scheduling
product_dataset = Dataset("snowflake://sales_analytics.retail.products")
order_summary_dataset = Dataset("snowflake://sales_analytics.retail.order_summary")

with DAG(
    "snowflake_dag_3",
    description="Generate order summary from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=[product_dataset],  # Run when product_dataset is updated
    tags=["simple_snowflake_batch"],
    catchup=False,
) as dag:
    # Task to get order summary
    order_summary = SQLExecuteQueryOperator(
        task_id="order_summary",
        sql="""
        SELECT * FROM CUSTOMERS;
        """,
        conn_id=SNOWFLAKE_CONN_ID,
        outlets=[order_summary_dataset],
    )
