from airflow import DAG
from pendulum import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    "catchup_test_dag",
    description="A DAG to test catchup functionality",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Run daily
    catchup=True,
) as dag:

    # Sample task
    sample_task_1 = EmptyOperator(
        task_id="fetch_customers",
    )
    
    # Sample task
    sample_task_2 = EmptyOperator(
        task_id="sample_task_2",
    )

    sample_task_1 >> sample_task_2
    