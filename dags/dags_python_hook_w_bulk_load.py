from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.customized_postgres_hook import CustomPostgresHook

with DAG(
        dag_id='dags_python_hook_w_bulk_load',
        start_date=pendulum.datetime(2023, 9, 1, tz="US/Eastern"),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl_nm':'TbCollegeSchoolInfo_bulk2',
                   'file_nm':'/opt/airflow/files/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash}}/CollegeSchoolInfo.csv'}
    )