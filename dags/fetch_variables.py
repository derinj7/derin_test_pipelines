from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "fetch_variables_dag",
    default_args=default_args,
    description="A DAG to fetch Airflow variables using Jinja templating",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["variables", "jinja"],
)

# Create a BashOperator task that uses Jinja templating to fetch the variables
fetch_variables_task = BashOperator(
    task_id="fetch_variables",
    bash_command="""
    echo "Fetching variables..."
    echo "DATABASE_HOST: {{ var.value.DATABASE_HOST }}"
    echo "DATABASE_USER: {{ var.value.DATABASE_USER }}"
    
    # For secrets, we can use the var.json approach which masks the values in logs
    echo "DATABASE_PASSWORD: {{ var.json.DATABASE_PASSWORD }}"
    
    echo "API_ENDPOINT: {{ var.value.API_ENDPOINT }}"
    echo "DEBUG_MODE: {{ var.value.DEBUG_MODE }}"
    
    # Use the variables in a sample command
    echo "Connecting to database at {{ var.value.DATABASE_HOST }} as {{ var.value.DATABASE_USER }}..."
    
    # Example of how you might use these in a real command (commented out)
    # psql -h {{ var.value.DATABASE_HOST }} -U {{ var.value.DATABASE_USER }} -d mydb
    """,
    dag=dag,
)

fetch_env = BashOperator(
    task_id="fetch_env",
    bash_command='echo "DATABASE_USER environment variable: {{ env().DATABASE_USERR }}"',
    dag=dag,
)


fetch_variables_task
