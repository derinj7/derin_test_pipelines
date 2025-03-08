from airflow import DAG
from pendulum import datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


SNOWFLAKE_TABLE = "ALL_DAYS"
SNOWFLAKE_CONN_ID = "my_snowflake_conn"


with DAG(
    "snowflake_example",
    doc_md=__doc__,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    sample_query = SQLExecuteQueryOperator(
        task_id="sample_query",
        sql="SELECT * FROM ALL_DAYS LIMIT 10;",
        conn_id="my_snowflake_conn",
    )
   
    sample_query