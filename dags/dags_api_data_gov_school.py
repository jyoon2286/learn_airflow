from operators.api_data_gov_to_csv_operator import DatagovApitoCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_api_dat_gov_school',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Eastern"),
    catchup=False
) as dag:   
    
    '''College school information'''
    tb_school_info = DatagovApitoCsvOperator(
        task_id='tb_school_info',
        dataset_nm='ed/collegescorecard/v1/schools?api_key=',
        path='/opt/airflow/files/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash }}',
        file_name='CollegeSchoolInfo.csv'
    )
      
tb_school_info