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
ARCHIVAL_BUCKET = "airflow-test-bucket-1107"
yesterday_dt = datetime.strptime('20210101', '%Y%m%d')
# today_dt = datetime.now().date()
# manifest_history_file_dates = set()
today_dt = datetime.now().date()

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


# def check_history_manifest_files_for_run_date():
#     global manifest_history_file_dates
#     prefix = "manifest/"
#     blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
#     yesterdays_dt = today_dt - timedelta(1)
#     for blob in blobs:
#         file_split = blob.name.split("-")
#         if len(file_split) > 1:
#             file_dt = datetime.strptime(file_split.split("-")[2], "%Y%m%d%H").date()
#             file_ts = datetime.strptime(file_split.split("-")[3], "%Y%m%d%H%M%S").date()
#             if file_dt < yesterdays_dt and (yesterdays_dt <= file_ts <= today_dt + timedelta(1)):
#                 manifest_history_file_dates.add(file_dt)
#     manifest_history_file_dates = list(manifest_history_file_dates)


def find_manifest_files_from_archive(**kwargs):
    import re
    prefix = "manifest"
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=prefix, delimiter="/")
    kv_feed = []
    standard_feed = []

    kv_feed_regex = prefix + "auction_kv_labels_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + "standard_feed_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"

    for blob in blobs:
        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)

    return {"kv_feed": kv_feed, "standard_feed": standard_feed}


def fetch_manifest_files_from_archive(**kwargs):
    xComm_var = kwargs['ti']
    manifest_files = xComm_var.xcom_pull(task_ids='find_manifest_files_from_archive')

    kv_feeds = manifest_files['kv_feed']
    standard_feeds = manifest_files['standard_feed']

    # move kv feeds from archival to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=kv_feeds, destination_files=kv_feeds)

    # move standard feeds from archival to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=standard_feeds, destination_files=standard_feeds)
    return True


# supporting function for both standard and kv feeds
def fetch_feed_file_from_archive(common_prefix: str):
    # find directories with date eg : feeds/standard-feed/20220202/*
    blobs = storage_client.list_blobs(ARCHIVAL_BUCKET, prefix=common_prefix, delimiter="/")
    file_path = [str(blob.name) for blob in blobs]

    # move files back to ingress bucket
    move_file(ARCHIVAL_BUCKET, BUCKET_NAME, source_files=file_path, destination_files=file_path)

    return True


def fetch_kv_feeds_from_archival(**kwargs):
    kv_feed_common_path = "feeds/kv_feed/{date}/".format(date=yesterday_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)


def fetch_standard_feeds_from_archival(**kwargs):
    kv_feed_common_path = "feeds/standard_feed/{date}/".format(date=yesterday_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)


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

start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
)

find_manifest_files_from_archive_t1_1 = PythonOperator(
    task_id='find_manifest_files_from_archive',
    python_callable=find_manifest_files_from_archive,
    provide_context=True,
    dag=dag)

fetch_manifest_files_from_archive_t1_2 = PythonOperator(
    task_id='fetch_manifest_files_from_archive',
    python_callable=fetch_manifest_files_from_archive,
    provide_context=True,
    dag=dag)

fetch_kv_feeds_from_archival_t2_1 = PythonOperator(
    task_id='fetch_kv_feeds_from_archival',
    python_callable=fetch_kv_feeds_from_archival,
    dag=dag)

fetch_standard_feeds_from_archival_t3_1 = PythonOperator(
    task_id='fetch_standard_feeds_from_archival',
    python_callable=fetch_standard_feeds_from_archival,
    dag=dag)

end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
)
# def bug_solve(transaction_dt_plus_one):
#     global  today_dt, load_ts
#     if ...
#         today_dt = .....
#     else
#         today_dt = ...
#     load_ts = ...

