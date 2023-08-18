from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_test_shell",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2023, 8, 1, tz="US/Eastern"),
    catchup=False
) as dag:
    
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt//mnt/c/Users/fever/learn_airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt//mnt/c/Users/fever/learn_airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    t1_orange >> t2_avocado