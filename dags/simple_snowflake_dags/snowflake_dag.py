from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Updated connection ID to match what you'll define in Airflow
SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    "snowflake_sales_analytics",
    description="Fetch and analyze data from the sales_analytics database",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Task to query customer data
    fetch_customers = SQLExecuteQueryOperator(
        task_id="fetch_customers",
        sql="SELECT * FROM CUSTOMERS",
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to query product data
    fetch_products = SQLExecuteQueryOperator(
        task_id="fetch_products",
        sql="SELECT * FROM CUSTOMERS;",
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to get order summary
    order_summary = SQLExecuteQueryOperator(
        task_id="order_summary",
        sql="""
        SELECT * FROM CUSTOMERS;
        """,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Task to analyze sales by category
    sales_by_category = SQLExecuteQueryOperator(
        task_id="sales_by_category",
        sql="""
        SELECT * FROM CUSTOMERS;
        """,
        conn_id=SNOWFLAKE_CONN_ID,
    )

    # Define task dependencies
    fetch_customers >> fetch_products >> order_summary >> sales_by_category
