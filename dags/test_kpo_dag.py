import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Get the namespace from Airflow configuration
namespace = conf.get("kubernetes", "NAMESPACE")

# Define compute resources for the Pod
compute_resources = k8s.V1ResourceRequirements(
    limits={"cpu": "500m", "memory": "1Gi"},
    requests={"cpu": "500m", "memory": "1Gi"}
)

with DAG(
    'test_kubernetes_pod_operator',
    default_args=default_args,
    description='A simple DAG to test KubernetesPodOperator',
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['test', 'kubernetes'],
) as dag:

    # Simple task that runs a basic Python container and prints a message
    basic_task = KubernetesPodOperator(
        namespace=namespace,
        image="python:3.9-slim",
        cmds=["python", "-c"],
        arguments=["print('Hello from KubernetesPodOperator!')"],
        labels={"app": "kpo-test"},
        name="basic-kpo-pod",
        task_id="basic_kpo_task",
        container_resources=compute_resources,
        get_logs=True,
        in_cluster=True,
    )
    
    # A task that uses Ubuntu image to run a shell command
    shell_task = KubernetesPodOperator(
        namespace=namespace,
        image="ubuntu:20.04",
        cmds=["bash", "-c"],
        arguments=["echo 'Current date: ' && date && echo 'Environment: ' && env | sort && sleep 10"],
        labels={"app": "kpo-shell-test"},
        name="shell-kpo-pod",
        task_id="shell_kpo_task", 
        container_resources=compute_resources,
        get_logs=True,
        in_cluster=True,
    )
    
    # Task ordering
    basic_task >> shell_task