from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.datasets import Dataset

# Updated connection ID
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

# Define dataset for data-aware scheduling
customer_dataset = Dataset("snowflake://sales_analytics.retail.customers")

with DAG(
    "snowflake_dag_1",
    description="Fetch customer data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule="0 18 * * *",  # Run daily at 6 PM
    catchup=False,
) as dag:
    # Task to query customer data
    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        sql="SELECT * FROM sales_analytics.retail.customers;",
        conn_id="wrong_snowflake_connection",  # INTENTIONAL ERROR: Using incorrect connection ID to make test fail
        outlets=[customer_dataset],
    )
