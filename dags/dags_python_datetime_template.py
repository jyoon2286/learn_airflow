from airflow import DAG
import pendulum
import datetime
from pprint import pprint 
from airflow.decorators import task

with DAG(
    dag_id="dags_python_datetime_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 8, 10, tz="US/Eastern"),
    catchup=True
) as dag:
    
    @task(task_id='python_task')
    def datetime_template(**kwargs):
        pprint(kwargs)

    datetime_template()