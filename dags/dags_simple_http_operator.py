from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum
import json

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False,
    schedule=None
) as dag:
    
    '''College Score Information'''
    fuel_stations = SimpleHttpOperator(
        task_id='fuel_stations',
        http_conn_id='data.gov',
        endpoint='/api/alt-fuel-stations/v1.json?limit=1&api_key={{var.value.apikey_data_gov}}',
        method='GET'
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='fuel_stations')
        import json
        from pprint import pprint       
        pprint(json.loads(rslt))
        
    fuel_stations >> python_2()