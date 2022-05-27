from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from airflow.operators.dummy import DummyOperator
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"  # do not use

# global variables
start_date = datetime(2022, 2, 25)
BUCKET_NAME = "airflow-test-bucket-1107"
DESTINATION_BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()

yesterday_dt = datetime.strptime('20210101', '%Y%m%d')
#today_dt = datetime.now().date()
manifest_history_file_list = []

"""
General Function
"""


def move_file(bucket_name: str, destination_bucket_name: str, source_files: list, destination_files: list):
    """Moves a blob from one bucket to another with a new name."""

    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    for source_path, destination_path in zip(source_files, destination_files):
        source_blob = source_bucket.blob(source_path)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_path
        )
        # source_bucket.delete_blob(source_path)

        print(
            "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )


"""
GENERAL FUNCTION FOR ARCHIVE MANIFEST 
"""


def check_history_files():
    global manifest_history_file_list
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
    yesterdays_dt = yesterday_dt  # today
    for blob in blobs:
        file_split = blob.name.split("-")
        if len(file_split) > 1:
            file_dt = datetime.strptime(file_split.split("-")[2], "%Y%m%d%H").date()

        if file_dt < yesterdays_dt:
            manifest_history_file_list.append(blob.name)


"""
GENERAL FUNCTION FOR ARCHIVE FEED AVRO FILES
"""


def archive_avro_files(avro_dt_directory_path):
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=avro_dt_directory_path)
    blobs_list = [blob.name for blob in blobs]
    move_file(bucket_name=BUCKET_NAME, destination_bucket_name='airflow-archive-1108', source_files=blobs_list,
              destination_files=blobs_list)
    return None


def check_and_archive_history_avro_files(common_file_path: str):
    today_dt_int = int(yesterday_dt.strftime('%Y%m%d'))
    global avro_files_list
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=common_file_path, delimiter="/",
                                      include_trailing_delimiter=True)
    for blob in blobs:
        file_path = blob.name

        # split and filter null spaces in the split list
        file_path_split = file_path.split("/")
        file_path_split = list(filter(None, file_path_split))

        #  compare the date of the directory and if less than today archive whole directory
        #  eg(feeds/standard_feed/20210101/* whole directory will be moved)
        if len(file_path_split) == 3 and int(file_path_split[-1]) < today_dt_int:
            archive_avro_files(file_path)


"""
DAG DEFINITION
"""

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

dag = DAG('adhoc_dag',
          schedule_interval=None,
          default_args=default_args,
          catchup=False
          )





def archive_history_kv_feeds():
    check_and_archive_history_avro_files(common_file_path='feeds/kv_feed/')


def archive_history_standard_feeds():
    check_and_archive_history_avro_files(common_file_path='feeds/standard_feed/')


def archive_history_manifest_files():
    global manifest_history_file_list
    # get history files list
    check_history_files()

    bucket_name = BUCKET_NAME
    destination_bucket_name = "airflow-archive-1108"

    move_file(bucket_name, destination_bucket_name,
              source_files=manifest_history_file_list,
              destination_files=manifest_history_file_list)

    return True



start = DummyOperator(
        task_id="start",
        trigger_rule="all_success",
    )
archive_kv_feeds_t1_1 = PythonOperator(
    task_id='archive_history_kv_feeds',
    python_callable=archive_history_kv_feeds,
    dag=dag)

archive_standard_feeds_t1_2 = PythonOperator(
    task_id='archive_history_standard_feeds',
    python_callable=archive_history_standard_feeds,
    dag=dag)

archive_manifest_files_t1_3 = PythonOperator(
    task_id='archive_history_manifest_files',
    python_callable=archive_history_manifest_files,
    dag=dag)

end = DummyOperator(
        task_id="end",
        trigger_rule="all_success",
        dag = dag
    )

start >> [archive_kv_feeds_t1_1, archive_standard_feeds_t1_2, archive_manifest_files_t1_3] >> end