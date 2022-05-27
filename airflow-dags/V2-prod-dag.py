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

dag = DAG("prod-dag-v2", default_args=default_args,
          schedule_interval=None,
          dagrun_timeout=timedelta(seconds=2),
          catchup=False)

"""
GENERAL FUNCTIONS
"""
def insert_row_into_table(bq_client, tablename:str, rows:list):
    # insert into the bigquery table
    errors = bq_client.insert_rows_json(tablename, rows)  # Make an API request.
    if errors == []:
        print("New rows have been added to {table}".format(table=tablename))
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def change_control_table(bq_client, transaction_dt, processed_flag):
    dml_statement = (
        "UPDATE `mineral-order-337219.test_dags.control_table` \
        SET processed_flag = {processed_flag} \
        WHERE transaction_dt = '{transaction_dt}'".format(transaction_dt=transaction_dt,processed_flag=processed_flag))

    query_job = bq_client.query(dml_statement)  # API request
    query_job.result()  # Waits for statement to finish


def filter_duplicate_hours(feed_path)->dict:
    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in feed_path:
        try:
            file_split = filepath.split("-")
            key = file_split[0] + file_split[2]
            if key in filter_hours_dict:
                time_stamp_previous_file = int(filter_hours_dict[key][3].split(".")[0])
                time_stamp_current_file = int(file_split[3].split(".")[0])

                if time_stamp_previous_file < time_stamp_current_file:
                    duplicate_file_list.append("-".join(filter_hours_dict[key]))
                    filter_hours_dict[key] = file_split
                else:
                    duplicate_file_list.append(filepath)
            else:
                filter_hours_dict[key] = file_split
        except Exception as e:
            print("Exception:,", e)

    processed_files = []

    for key in filter_hours_dict.keys():
        processed_files.append("-".join(filter_hours_dict[key]))
    return {"processed_files": processed_files, "duplicate_file_list": duplicate_file_list}

def scan_and_load_file_to_table():
    rows_to_insert = []
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix)
    blobs_list = [str(blob.name) for blob in blobs]
    filtered_files = filter_duplicate_hr(blobs_list)
    for file_path in filtered_files['processed_files']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hr = parsed_date_stamp.hour
        duplicate_file_check = "N"
        rows_to_insert.append({"manifest_name": manifest_name, "transaction_dt": transaction_dt,
                               "transaction_hr": transaction_hr,"duplicate_file_check": duplicate_file_check,
                               "load_ts": load_ts})

    for file_path in filtered_files['duplicate_file_list']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hr = parsed_date_stamp.hour
        duplicate_file_check = "N"
        rows_to_insert.append({"manifest_name": manifest_name, "transaction_dt": transaction_dt,
                               "transaction_hr": transaction_hr,"duplicate_file_check": duplicate_file_check,
                               "load_ts": load_ts})

    return rows_to_insert

def truncate_bq_table(bq_client, tablename):
    query_str = f"TRUNCATE TABLE {tablename}"
    query_job = bq_client.query(query_str)
    result = query_job.result()
    return result

def load_manifest_stagging_table():
    from google.cloud import bigquery

    # Construct a BigQuery client object.
    client = bigquery.Client()

    #truncate_bq_table(client, "mineral-order-337219.test_dags.log_manifest_stg")

    rows = scan_and_load_file_to_table()
    insert_row_into_table(client, "mineral-order-337219.test_dags.log_manifest_stg", rows)


def is_history_date(bq_client, transaction_dt):
    query = "SELECT transaction_dt, processed_flag from `mineral-order-337219.test_dags.control_table` " \
            "WHERE transaction_dt = '{transaction_dt}'".format(transaction_dt=transaction_dt)
    query_job = bq_client.query(query)
    results = query_job.result()
    if results.total_rows != 0:
        print("date Exists")
        return True
    return False


def load_control_table():
    from google.cloud import bigquery
    client = bigquery.Client()
    rows_insert_for_control_table = []
    query = "SELECT count(filename) as count, transaction_dt \
            FROM `mineral-order-337219.test_dags.log_manifest_stg` \
            GROUP BY transaction_dt"

    query_job = client.query(query)
    results = query_job.result()
    for result in results:
        if result.count >= 48:
            rows_insert_for_control_table.append({"transaction_dt": result.transaction_dt, "processed_flag": "false"})
        elif is_history_date(client, result.transaction_dt):
            # update the processed flag to false to again process the history files
            change_control_table(client, result.transaction_dt, 'false')


    # insert new transaction dates into control table
    insert_row_into_table(client, rows_insert_for_control_table)

start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
    dag=dag
)

# scan the manifest files and insert into manifest stagging table
task_1 = PythonOperator(
    task_id='load_manifest_stagging_table',
    python_callable=load_manifest_stagging_table,
    dag=dag
)

# detect the new transaction_dt and history_dt and ignore future dates
task_2 = PythonOperator(
    task_id='load_control_table',
    python_callable=load_control_table,
    dag=dag)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)

start >> task_1 >> task_2 >> end