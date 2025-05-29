from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "snowflake_default"

# Define datasets for data-aware scheduling
customer_dataset = Dataset("snowflake://sales_analytics.retail.customers")
product_dataset = Dataset("snowflake://sales_analytics.retail.products")

with DAG(
    "snowflake_dag_2",
    description="Fetch product data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=[customer_dataset],  # Run when customer_dataset is updated
    tags=["simple_snowflake_batch"],
    catchup=False,
) as dag:
    # Task to query product data
    fetch_products = SQLExecuteQueryOperator(
        task_id="fetch_products",
        sql="SELECT * FROM CUSTOMERS;",
        conn_id=SNOWFLAKE_CONN_ID,
        outlets=[product_dataset],
    )
