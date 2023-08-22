import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import register2


with DAG(
    dag_id="dags_python_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False
) as dag:

    register2_t1 = PythonOperator(
        task_id='register2_t1',
        python_callable = register2,
        op_args=['Jae', 'M', 'DC', 'US'],
        op_kwargs={'email':'jae123@gmail.com', 'phone':'123-123-1234'},
    )

    register2_t1