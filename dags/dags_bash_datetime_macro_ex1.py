from __future__ import annotations
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_bash_datetime_macro_ex1",
    schedule="0 0 L * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False,
) as dag:
    bash_t1 =  BashOperator(
        task_id = 'bash_t1',
        env={'START_DATE': '{{ data_interval_start.in_timezone("US/Eastern") | ds }}',
             'END_DATE': '{{ (data_interval_end.in_timezone("US/Eastern") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}} '
             },
        bash_command= 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE "'
    )
