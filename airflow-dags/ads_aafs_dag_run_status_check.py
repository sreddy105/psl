#Import library
import os
import airflow
import re
import logging
import time
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators import dummy_operator
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator

#Config variables
ENVIRONMENT = os.getenv("env").upper()
AAFS_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')

DAG_NAME="ads_aafs_dag_run_status_check"
target_dag_id="ads_reference_gcs_to_bq_data_load_test"

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

#Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': '2023-06-23',
    'email': 'surendra.gangavaram.ctr@sabre.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

#Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='AAFS DAG run status check',
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs= 1)

def dag_run_status_check():
    waiting_time=1*1*60 # hours * mins * seconds
    timeout=1*2*60
    counter=timeout/waiting_time
    dag_runs = DagRun.find(dag_id=target_dag_id) # checking the DAG state, dag_runs returns all the available states from the metadata database
    while dag_runs[-1].get_state() == 'running' and counter>0: 
        print("DAG State:",dag_runs[-1].get_state())
        print("Will wait for",waiting_time,"seconds")
        time.sleep(waiting_time)        
        dag_runs = DagRun.find(dag_id=target_dag_id)
        counter=counter-1
    print("DAG State-2:",dag_runs[-1].get_state())

check_dag_run_status_check= PythonOperator(
    task_id='dag_run_status_check',
    python_callable=dag_run_status_check,
    dag=dag)

