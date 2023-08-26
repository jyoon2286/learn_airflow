from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException

import pendulum

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    schedule=None,
    catchup=False
) as dag:
    @task.branch(task_id='branching')
    def random_branch():
        import random
        item_lst = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'

    task_a = BashOperator(
        task_id='task_a',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('Succeed')


    @task(task_id='task_c')
    def task_c():
        print('Succeed')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('Succeed')

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()