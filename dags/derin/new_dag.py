from airflow.decorators import dag, task

@dag(dag_id="something_new", catchup=False)
def sample_dag():

    @task
    def sample_task():
        print("Printing something")

    sample_task()

sample_dag()
