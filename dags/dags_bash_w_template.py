from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_w_template",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False,
) as dag:
    bash_t1 =  BashOperator(
        task_id = 'bash_t1',
        bash_command= 'echo "data_interval end: {{ data_interval_end }} "'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env={
            'START_DATE':'{{ data_interval_start | ds }}',
            'END_DATE':'{{ data_interval_end | ds }}',
        },
        bash_command= 'echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2