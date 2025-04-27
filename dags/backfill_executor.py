from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "backfill_executor",
    default_args=default_args,
    description="Run backfill for any DAG using parameters",
    schedule_interval=None,  # This is a manually triggered DAG
    start_date=days_ago(1),
    catchup=False,
    params={
        "dag_id": "",  # This will be passed at runtime
        "start_date": "",  # Format: YYYY-MM-DD
        "end_date": "",  # Format: YYYY-MM-DD
    },
)

# BashOperator that runs the airflow backfill command
backfill_task = BashOperator(
    task_id="run_backfill",
    bash_command="airflow dags backfill {{ params.dag_id }} --start-date {{ params.start_date }} --end-date {{ params.end_date }}",
    dag=dag,
)

# Just one task in this DAG
backfill_task
