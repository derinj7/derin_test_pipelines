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
    
    # Variables to fetch (without the AIRFLOW_VAR_ prefix as Airflow strips it)
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
            value = Variable.get(key, default_var=None)
            
            if value is None:
                logger.info(f"{key}: Not found")
                continue
                
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
    
    # Let's also print all available variables for debugging
    logger.info("=== All available Airflow variables ===")
    all_vars = Variable.get_all()
    for var_name, var_val in all_vars.items():
        # Mask sensitive values
        if any(sensitive in var_name.upper() for sensitive in ['API', 'KEY', 'SECRET', 'PASSWORD', 'URL', 'TOKEN']):
            if var_val and len(var_val) > 8:
                masked = var_val[:4] + '*' * (len(var_val) - 8) + var_val[-4:]
            else:
                masked = '*' * len(var_val) if var_val else 'None'
            logger.info(f"{var_name}: {masked}")
        else:
            logger.info(f"{var_name}: {var_val}")
    
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
