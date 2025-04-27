from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Define datasets for data-aware scheduling
customer_dataset = Dataset("snowflake://sales_analytics.retail.customers")
product_dataset = Dataset("snowflake://sales_analytics.retail.products")

with DAG(
    "snowflake_dag_2",
    description="Fetch product data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=[customer_dataset],  # Run when customer_dataset is updated
    catchup=False,
) as dag:
    # Task to query product data
    fetch_products = SQLExecuteQueryOperator(
        task_id="fetch_products",
        sql="SELECT * FROM sales_analytics.retail.products;",
        conn_id=SNOWFLAKE_CONN_ID,
        outlets=[product_dataset],
    )
