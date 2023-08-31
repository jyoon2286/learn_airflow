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
    college_score_info = SimpleHttpOperator(
        task_id='college_score_info',
        http_conn_id='data.gov',
        endpoint='/api/alt-fuel-stations/v1.json?limit=1&api_key={{var.value.apikey_data_gov}}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='college_score_info')
        import json
        from pprint import pprint
        json_data = json.loads(rslt)
        
        pprint(json.loads(rslt))
        
    college_score_info >> python_2()