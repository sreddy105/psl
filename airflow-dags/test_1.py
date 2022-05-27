from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator


start_date = datetime(2022, 2, 25)
default_args = {
    "owner": "xyz",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


dag = DAG('test_2_simple',
          schedule_interval= None,
          default_args=default_args,
          catchup=False
          )

def my_processing_func(**kwargs):
    print("I have sensed the task is complete in a dag")


some_task = PythonOperator(
    task_id='task_1',
    python_callable=my_processing_func,
    dag=dag)
