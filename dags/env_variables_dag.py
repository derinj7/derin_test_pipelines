from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def display_variables(**kwargs):
    """
    Function to display the Airflow variables set from the .env file 
    """
    logger = logging.getLogger(__name__)
    
    # Variables to fetch (corresponding to the ones set in the CI/CD)
    variable_keys = [
        'ENVIRONMENT',
        'API_KEY',
        'DATABASE_URL',
        'DEPLOYMENT_DATE',
        'DEPLOYED_BY',
        'COMMIT_SHA'
    ]
    
    logger.info("=== Environment Variables from CI/CD ===")
    
    for key in variable_keys:
        try:
            # Try to get variable from Airflow Variables
            value = Variable.get(key, default_var=f"Variable {key} not found")
            
            # Mask sensitive information
            if key in ['API_KEY', 'DATABASE_URL']:
                if value and len(value) > 8:
                    masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:]
                else:
                    masked_value = '*' * len(value) if value else 'None'
                logger.info(f"{key}: {masked_value}")
            else:
                logger.info(f"{key}: {value}")
                
        except Exception as e:
            logger.error(f"Error retrieving variable {key}: {str(e)}")
    
    return "Variables displayed in logs"

with DAG(
    'display_env_variables',
    default_args=default_args,
    description='DAG to display environment variables set from CI/CD',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['variables', 'environment'],
) as dag:
    
    display_task = PythonOperator(
        task_id='display_variables',
        python_callable=display_variables,
        provide_context=True,
    )
    
    # Task dependencies
    display_task
