from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' Generating the first Taskgroup using a decorator'''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('First task in the first TaskGroup')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':' Second task in the first TaskGroup'}
        )

        inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='Generating the second Taskgroup using class') as group_2:
        ''' Testing Docstring method to see if it shows up in UI'''
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('First task in the second TaskGroup')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': 'Second task in the second TaskGroup'}
        )
        inner_func1() >> inner_function2

    group_1() >> group_2