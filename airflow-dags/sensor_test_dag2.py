from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
#from airflow.operators.sensors import ExternalTaskSensor for old
from airflow.sensors.external_task_sensor import ExternalTaskSensor

start_date = datetime(2022, 2, 25)
default_args = {
    "owner": "dhruv",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG('external_task_sensor_dag_2',
          schedule_interval= '58 21 * * *',
          default_args=default_args,
          catchup=False
          )


def prev_execution_dt(execution_date, **kwargs):
    weekday=execution_date.strftime('%A')
    print(weekday)
    if weekday == "Thursday":
        execution_dt_derived=execution_date - timedelta(hours=72)
        print(execution_dt_derived)
    else:
        execution_dt_derived=execution_date - timedelta(hours=24)
        print(execution_dt_derived)
    return execution_dt_derived

external_task_sensor = ExternalTaskSensor(
    task_id='external_task_sensor',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    external_task_id='archive_duplicate_files',
    external_dag_id='hook_gcp_bucket_final_dev',
    allowed_states=['success'],
    #execution_delta=timedelta(hours=24),
    execution_date_fn=lambda dt: start_date,
    dag=dag)


def my_processing_func(**kwargs):
    print("I have sensed the task is complete in a dag")


some_task = PythonOperator(
    task_id='task_1',
    python_callable=my_processing_func,
    dag=dag)

some_task