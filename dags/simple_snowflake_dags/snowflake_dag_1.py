from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Updated connection ID
SNOWFLAKE_CONN_ID = "snowflake_default"


with DAG(
    "snowflake_dag_1",
    description="Fetch customer data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",  # Run every hour
    tags=["simple_snowflake_batch"],
    catchup=False,
) as dag:
    # Task to query customer data
    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        sql="SELECT * FROM CUSTOMERS;",
        conn_id="snowflake_default",  
    )
