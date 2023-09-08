from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id='dags_file_sensor',
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Eastern"),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    collegeSchoolInfo = FileSensor(
        task_id='collegeSchoolInfo',
        fs_conn_id='conn_file_opt_airflow_files',
        filepath='/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash}}/CollegeSchoolInfo.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24, # for one day 
        mode='reschedule'
    )