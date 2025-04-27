from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    'display_variables',
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
echo "=== Airflow Variables from CI/CD using Jinja Templating ==="
echo "environment: {{ var.value.environment | default('Not found') }}"
echo "api_key: {{ var.value.api_key[:4] }}*****{{ var.value.api_key[-4:] if var.value.api_key is defined and var.value.api_key|length > 8 else '****' }}"
echo "database_url: {{ var.value.database_url[:4] }}*****{{ var.value.database_url[-4:] if var.value.database_url is defined and var.value.database_url|length > 8 else '****' }}"
echo "deployment_date: {{ var.value.deployment_date | default('Not found') }}"
echo "deployed_by: {{ var.value.deployed_by | default('Not found') }}"
echo "commit_sha: {{ var.value.commit_sha | default('Not found') }}"
        ''',
    )
    
    
    # Define task dependencies
    display_task
