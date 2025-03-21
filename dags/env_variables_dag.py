from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def log_variables(**kwargs):
    """
    Simple function to log the task instance and context
    """
    logger = logging.getLogger(__name__)
    logger.info("Task instance: %s", kwargs.get('task_instance'))
    logger.info("Variables accessed via Jinja templating in the BashOperator task")
    return "Variables accessed using Jinja templating"

with DAG(
    'display_env_variables',
    default_args=default_args,
    description='DAG to display environment variables using Jinja templating',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['variables', 'environment'],
) as dag:
    
    # Use Jinja templating to access variables directly in the BashOperator
    display_task = BashOperator(
        task_id='display_variables',
        bash_command='''
echo "=== Environment Variables from CI/CD using Jinja Templating ==="
echo "environment: {{ var.value.environment | default('Not found') }}"
echo "api_key: {{ var.value.api_key[:4] }}*****{{ var.value.api_key[-4:] if var.value.api_key is defined and var.value.api_key|length > 8 else '****' }}"
echo "database_url: {{ var.value.database_url[:4] }}*****{{ var.value.database_url[-4:] if var.value.database_url is defined and var.value.database_url|length > 8 else '****' }}"
echo "deployment_date: {{ var.value.deployment_date | default('Not found') }}"
echo "deployed_by: {{ var.value.deployed_by | default('Not found') }}"
echo "commit_sha: {{ var.value.commit_sha | default('Not found') }}"
        ''',
    )
    
    # Log task completion
    log_task = PythonOperator(
        task_id='log_completion',
        python_callable=log_variables,
        provide_context=True,
    )
    
    # Define task dependencies
    display_task >> log_task
