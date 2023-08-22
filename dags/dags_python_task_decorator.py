import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator


with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False,
) as dag:
    
    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(ex_input):
        print(ex_input)



    python_task_1 = print_context('Running task decorator')