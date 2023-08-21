from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 9 1 * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='sending_email_task',
        to='jyoon2286@gmail.com',
        subject='Testing Airflow email operator',
        html_content='Airflow email operator is working'
    )