import time
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from airflow.operators.dummy import DummyOperator
import os
from airflow.api.common.experimental import get_task_instance
from airflow.models.dagrun import DagRun

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"  # do not use

# global variables
start_date = datetime(2022, 2, 25)
BUCKET_NAME = "airflow-test-bucket-1107"
DESTINATION_BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()
today_dt = datetime.now().date()
manifest_history_file_list = []

load_ts: datetime

start_date = datetime(2022, 2, 25)
default_args = {
    "owner": "dhruv-airflow-test-dev",
    "depends_on_past": False,
    "start_date": start_date,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("dev_dag_patch", default_args=default_args,
          schedule_interval='07 18 * * *',
          dagrun_timeout=timedelta(seconds=2),
          catchup=False)

global_load_ts = "{{{ yesterday_ds_nodash }}}"


def test_1(**kwargs):
    print(global_load_ts)
    return True


def get_last_exec_date(dag_id='dev_dag_patch', **kwargs):
    template_fields = ('dag_run')
    print("Execution", kwargs['execution_date'])
    print("ACUTAL TIME WITHOUT TEMPLATE:", kwargs['dag_run'].start_date)
    # print("WITH TEMPLATE", kwargs['dag_run']['start_date'])
    print(dag.get_last_dagrun(include_externally_triggered=True))
    print("ACTUAL TIME", kwargs['key1'])
    print("DAG RUN", dag.start_date)
    return True


python_sleep_1 = PythonOperator(
    task_id='python_sleep',
    python_callable=test_1,
    dag=dag)

test_2 = PythonOperator(
    task_id='test_2',
    python_callable=get_last_exec_date,
    op_kwargs={'key1': "{{ dag_run.start_date }}"},
    provide_context=True,
    dag=dag
)

# archive_duplicate_files_t4_2 = PythonOperator(
#     task_id='get_load_and_today_dt_2',
#     python_callable=get_load_and_today_dt,
#     provide_context=True,
#     dag=dag)
