from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_trigger_rule_ex1',
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    schedule=None,
    catchup=False
) as dag:
    bash_upstream_1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')


    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('Succeed')

    @task(task_id='python_downstream_1', trigger_rule='all_done')
    def python_downstream_1():
        print('Succeed')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()