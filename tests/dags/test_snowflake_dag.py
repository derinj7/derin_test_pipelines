"""
Test file for snowflake_dag_1.py.
This test ensures that the Snowflake DAG is properly configured,
has no import errors, and meets the required standards.
"""

import os
import logging
from contextlib import contextmanager
import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_dag_bag():
    """Get a DagBag instance for testing"""
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


def test_snowflake_dag_loaded():
    """Test that the snowflake_dag_1 is loaded correctly"""
    dag_bag = get_dag_bag()
    assert "snowflake_dag_1" in dag_bag.dags, "snowflake_dag_1 not found in loaded DAGs"
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_snowflake_dag_structure():
    """Test the structure of snowflake_dag_1"""
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag("snowflake_dag_1")
    
    # Test that the dag has the correct tasks
    task_ids = [task.task_id for task in dag.tasks]
    assert "fetch_customers" in task_ids, "fetch_customers task not found in snowflake_dag_1"
    
    # Test the fetch_customers task configuration
    fetch_task = dag.get_task("fetch_customers")
    assert fetch_task.conn_id == "my_snowflake_conn", "Incorrect connection ID for fetch_customers task"
    assert "SELECT * FROM sales_analytics.retail.customers;" in fetch_task.sql, "Incorrect SQL in fetch_customers task"


def test_snowflake_dag_schedule():
    """Test that the schedule is set correctly for snowflake_dag_1"""
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag("snowflake_dag_1")
    assert dag.schedule_interval == "0 18 * * *", "Incorrect schedule interval for snowflake_dag_1"


def test_snowflake_dag_dataset():
    """Test that the dataset is defined correctly for snowflake_dag_1"""
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag("snowflake_dag_1")
    
    # Find tasks with outlets
    tasks_with_outlets = [task for task in dag.tasks if hasattr(task, 'outlets') and task.outlets]
    
    # Verify that at least one task has the expected dataset as an outlet
    dataset_uris = []
    for task in tasks_with_outlets:
        dataset_uris.extend([dataset.uri for dataset in task.outlets])
    
    assert "snowflake://sales_analytics.retail.customers" in dataset_uris, "Expected dataset not found in outlets"


def test_snowflake_dag_missing_tags():
    """Test that the DAG has tags (currently missing in the provided DAG)"""
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag("snowflake_dag_1")
    
    # This test will fail because the DAG doesn't have tags,
    # highlighting that tags should be added
    assert dag.tags, "snowflake_dag_1 has no tags"


def test_snowflake_dag_missing_retries():
    """Test that the DAG has retries set (currently missing in the provided DAG)"""
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag("snowflake_dag_1")
    
    # This test will fail because the DAG doesn't have retries set,
    # highlighting that retries should be added
    assert dag.default_args and "retries" in dag.default_args, "snowflake_dag_1 has no retries configured"
    if dag.default_args and "retries" in dag.default_args:
        assert dag.default_args["retries"] >= 2, "snowflake_dag_1 must have task retries >= 2"
