import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    # daily 9 am job
    schedule=" 0 9 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False,
) as dag:
    def test_python_function():
        soccer_team = ['Tottenham', 'Arsenal', 'Chelsea', 'Manunited', 'Mancity', 'Newcastle']
        ran_int = random.randint(0,5)
        print(soccer_team[ran_int])

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=test_python_function 
    )


if __name__ == '__main__':
    py_t1