from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable, Connection
from datetime import datetime, timedelta
import os
import logging
import json

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
    
    # Variables to fetch - using lowercase as per the convention
    variable_keys = [
        'environment',
        'api_key',
        'database_url',
        'deployment_date',
        'deployed_by',
        'commit_sha'
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
            if 'key' in key.lower() or 'url' in key.lower() or 'password' in key.lower():
                if value and len(value) > 8:
                    masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:]
                else:
                    masked_value = '*' * len(value) if value else 'None'
                logger.info(f"{key}: {masked_value}")
            else:
                logger.info(f"{key}: {value}")
                
        except Exception as e:
            logger.error(f"Error retrieving variable {key}: {str(e)}")
    
    # Attempt to load variables directly from environment
    logger.info("=== Environment variables from os.environ ===")
    airflow_vars = {k[12:].lower(): v for k, v in os.environ.items() if k.startswith('AIRFLOW_VAR_')}
    
    if airflow_vars:
        for var_name, var_val in airflow_vars.items():
            # Mask sensitive values
            if any(sensitive in var_name.lower() for sensitive in ['api', 'key', 'secret', 'password', 'url', 'token']):
                if var_val and len(var_val) > 8:
                    masked = var_val[:4] + '*' * (len(var_val) - 8) + var_val[-4:]
                else:
                    masked = '*' * len(var_val) if var_val else 'None'
                logger.info(f"{var_name}: {masked}")
            else:
                logger.info(f"{var_name}: {var_val}")
    else:
        logger.info("No AIRFLOW_VAR_ environment variables found")
    
    # Try to find and load variables from files
    variables_loaded = False
    logger.info("=== Checking for variables file ===")
    var_file_paths = [
        '/usr/local/airflow/include/variables.json',
        '/opt/airflow/include/variables.json',
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'include', 'variables.json')
    ]
    
    for path in var_file_paths:
        if os.path.exists(path):
            logger.info(f"Found variables file at: {path}")
            try:
                with open(path, 'r') as f:
                    variables_data = json.load(f)
                logger.info(f"Successfully loaded {len(variables_data)} variables from file")
                logger.info(f"Variables from file: {', '.join(variables_data.keys())}")
                
                # Try to import the variables
                for var_name, var_value in variables_data.items():
                    try:
                        Variable.set(var_name, var_value)
                        logger.info(f"Set variable: {var_name}")
                        variables_loaded = True
                    except Exception as e:
                        logger.error(f"Failed to set variable {var_name}: {str(e)}")
            except Exception as e:
                logger.error(f"Error reading variables file: {str(e)}")
        else:
            logger.info(f"Variables file not found at: {path}")
    
    # If variables were loaded, display them again
    if variables_loaded:
        logger.info("=== Displaying newly set variables ===")
        for key in variable_keys:
            try:
                # Get the variable that we just set
                value = Variable.get(key, default_var=None)
                
                if value is None:
                    logger.info(f"{key}: Still not found after import")
                    continue
                    
                # Mask sensitive information
                if 'key' in key.lower() or 'url' in key.lower() or 'password' in key.lower():
                    if value and len(value) > 8:
                        masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:]
                    else:
                        masked_value = '*' * len(value) if value else 'None'
                    logger.info(f"{key}: {masked_value}")
                else:
                    logger.info(f"{key}: {value}")
                    
            except Exception as e:
                logger.error(f"Error retrieving variable {key} after import: {str(e)}")
    
    # Check for connections
    logger.info("=== Available Connections ===")
    try:
        from airflow.hooks.base import BaseHook
        connections = BaseHook.get_connections()
        if connections:
            for conn in connections:
                logger.info(f"Connection ID: {conn.conn_id}, Type: {conn.conn_type}, Host: {conn.host}")
        else:
            logger.info("No connections found")
    except Exception as e:
        logger.error(f"Error listing connections: {str(e)}")
    
    # Create a simple JSON summary of the variables for the task result
    summary = {}
    for key in variable_keys:
        try:
            value = Variable.get(key, default_var="Not found")
            
            # Don't include sensitive values in the summary
            if 'key' in key.lower() or 'url' in key.lower() or 'password' in key.lower():
                summary[key] = "******" if value != "Not found" else "Not found"
            else:
                summary[key] = value
        except:
            summary[key] = "Error retrieving"
            
    return f"Variables summary: {json.dumps(summary, indent=2)}"

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
