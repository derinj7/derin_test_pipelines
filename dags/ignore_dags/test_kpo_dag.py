import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# Get the namespace from Airflow configuration
namespace = conf.get("kubernetes", "NAMESPACE")

# Define compute resources for the Pod
compute_resources = k8s.V1ResourceRequirements(
    limits={"cpu": "500m", "memory": "1Gi"}, requests={"cpu": "500m", "memory": "1Gi"}
)

# Define pod override with custom resources (larger than default, smaller than max)
pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "5Gi"},
                    limits={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "5Gi"},
                ),
            )
        ]
    )
)

with DAG(
    "test_kubernetes_pod_operator",
    default_args=default_args,
    description="A simple DAG to test KubernetesPodOperator",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["test", "kubernetes"],
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
        arguments=[
            "echo 'Current date: ' && date && echo 'Environment: ' && env | sort && sleep 10"
        ],
        labels={"app": "kpo-shell-test"},
        name="shell-kpo-pod",
        task_id="shell_kpo_task",
        container_resources=compute_resources,
        get_logs=True,
        in_cluster=True,
    )

    # New task with pod override to test larger resource allocation
    # Using a Python data science image which has more dependencies but is still reasonable for testing
    override_task = KubernetesPodOperator(
        namespace=namespace,
        image="python:3.9-slim",  # Using a basic Python image for reliability
        cmds=["bash", "-c"],
        arguments=[
            """
# Install resource monitoring tools
pip install --no-cache-dir numpy
echo "Starting resource test with pod override..."
python -c "
import os
import sys
import platform
import numpy as np

# Create a larger array to use some memory
large_array = np.ones((1000, 1000))

# Print resource information
print('Python version:', sys.version)
print('Platform info:', platform.platform())
print('CPU count:', os.cpu_count())
print('Current working directory:', os.getcwd())
print('Disk space:')
os.system('df -h /')
print('Memory info:')
os.system('free -h')
print('Test array shape:', large_array.shape)
print('Test array memory usage (MB):', large_array.nbytes / (1024 * 1024))
print('Pod override test complete!')
"
            """
        ],
        labels={"app": "kpo-override-test"},
        name="override-kpo-pod",
        task_id="override_kpo_task",
        executor_config={
            "pod_override": pod_override
        },  # Apply the pod override configuration
        get_logs=True,
        in_cluster=True,
    )

    # Task ordering
    basic_task >> shell_task >> override_task
