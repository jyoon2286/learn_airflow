from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_bash_sensor',
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Eastern"),
    schedule='0 6 * * *',
    catchup=False
) as dag:




    sensor_task_by_poke = BashSensor(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airflow/files/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash}}/CollegeSchoolInfo.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=30,      #for 30 sec
        timeout=60*2,          #in 2 min
        mode='poke',
        soft_fail=False
    )

    sensor_task_by_reschedule = BashSensor(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash}}/CollegeSchoolInfo.csv'},
        bash_command=f'''echo $FILE && 
                        if [ -f $FILE ]; then 
                              exit 0
                        else 
                              exit 1
                        fi''',
        poke_interval=60*3,    # for 3 min
        timeout=60*9,          # in 9 min
        mode='reschedule',
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE': '/opt/airflow/files/CollegeSchoolInfo/{{data_interval_end.in_timezone("US/Eastern") | ds_nodash}}/CollegeSchoolInfo.csv'},
        bash_command='echo "num of running: `cat $FILE | wc -l`"',
    )

    [sensor_task_by_poke,sensor_task_by_reschedule] >> bash_task