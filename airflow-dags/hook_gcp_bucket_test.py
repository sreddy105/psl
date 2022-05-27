from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
import json

from airflow.contrib.hooks import gcs_hook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowSkipException

from google.api_core import exceptions
import re
import logging

connection = gcs_hook.GoogleCloudStorageHook(google_cloud_storage_conn_id='my_gcp_connection')
default_args = {
    "owner": "dhruv-airflow-test",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("hook_gcp_bucket_test", default_args=default_args, schedule_interval=None)


def get_manifest_json_file(**kwargs):
    files_list = connection.list('airflow-test-bucket-1107', prefix="manifest/"
                                 , delimiter=".json")
    manifest_json_files_list = [x for x in files_list if len(x.split("/")) == 2]
    if len(manifest_json_files_list) <1:
        raise AirflowSkipException
    return manifest_json_files_list


def filter_json_list(**kwargs):
    xComm_var = kwargs['ti']
    manifest_json_files_list = xComm_var.xcom_pull(task_ids='manifest_read_json_files')
    filter_hours_dict = {}
    corrupt_file_list = []
    for file in manifest_json_files_list:
        print()
        file_split = file.split("-")
        key = file_split[2]
        if key in filter_hours_dict.keys():
            # print(file_split)
            second_time_stamp_previous = int(filter_hours_dict[key][3].split(".")[0])
            second_time_stamp_current = int(file_split[3].split(".")[0])
            # print("CURRENT AND PREVIOUS : ", second_time_stamp_current, second_time_stamp_previous)
            if second_time_stamp_previous < second_time_stamp_current:
                corrupt_file_list.append("-".join(filter_hours_dict[key]))
                filter_hours_dict[key] = file_split
            else:
                corrupt_file_list.append(file)
        else:
            filter_hours_dict[file_split[2]] = file_split
    filtered_list = []
    for key in filter_hours_dict.keys():
        filtered_list.append("-".join(filter_hours_dict[key]))
    return {'filtered_list': filtered_list, 'corrupt_file_list': corrupt_file_list}


def process_manifest_json_file(**kwargs):
    xComm_var = kwargs['ti']
    manifest_json_files = xComm_var.xcom_pull(task_ids='filter_json_list')
    manifest_json_files_list = manifest_json_files['filtered_list']
    avro_files_list = []
    for file in manifest_json_files_list:
        try:
            data = connection.download(bucket_name="airflow-test-bucket-1107", object_name=file)
            # print(data)
            data = json.loads(data)
            # print(data['filepath'])
            path = data['path'][1:]
            avro_files_list.extend([ path + "/" +  filepath['name'] for filepath in data['files']])
        except exceptions.NotFound as e:
            logger = logging.getLogger("airflow.task")
            logger.error("Exception Not found:" + str(e))

    return avro_files_list


def check_valid_avro_filepath(**kwargs):
    bucket_name = 'airflow-test-bucket-1107'
    xComm_var = kwargs['ti']
    avro_files_list = xComm_var.xcom_pull(task_ids="process_manifest_json_file")
    valid_avro_files_bq = []
    valid_avro_files_archive = []
    for file in avro_files_list:
        if connection.exists(bucket_name=bucket_name, object_name=file):
            valid_avro_files_bq.append("gs://" + bucket_name + "/" + file)
            valid_avro_files_archive.append(file)
    return {"valid_files_bq": valid_avro_files_bq, "valid_files_archive": valid_avro_files_bq}


def load_bq(**kwargs):
    bucket_name = 'airflow-test-bucket-1107'
    BQ_CONN_ID = 'my_gcp_connection'
    xComm_var = kwargs['ti']
    valid_avro_file = xComm_var.xcom_pull(task_ids='check_valid_avro_filepath')

    valid_avro_files_list = valid_avro_file['valid_files_bq']
    # bq_operator = BigQueryHook(gcp_conn_id=BQ_CONN_ID, delegate_to=None)  # BQ_CONN_ID
    # print(bq_operator)
    return valid_avro_files_list


def archive_processed_json_files(**kwargs):
    bucket_name = 'airflow-test-bucket-1107'
    xComm_var = kwargs['ti']
    valid_avro_file = xComm_var.xcom_pull(task_ids='filter_json_list')
    valid_avro_files_archive = valid_avro_file['filtered_list']
    print(valid_avro_file)
    for file in valid_avro_files_archive:
        file_name = file.split("/")[-1]
        connection.copy(
            source_bucket="airflow-test-bucket-1107",
            source_object=file,
            destination_bucket="airflow-test-bucket-1107",
            destination_object="manifest/Archive/" + file_name
        )


def archive_corrupted_json_files(**kwargs):
    bucket_name = 'airflow-test-bucket-1107'
    xComm_var = kwargs['ti']
    valid_avro_file = xComm_var.xcom_pull(task_ids='filter_json_list')
    valid_avro_files_archive = valid_avro_file['corrupt_file_list']
    print(valid_avro_file)
    for file in valid_avro_files_archive:
        file_name = file.split("/")[-1]
        connection.copy(
            source_bucket="airflow-test-bucket-1107",
            source_object=file,
            destination_bucket="airflow-test-bucket-1107",
            destination_object="manifest/Archive/Corrupted_Files/" + file_name
        )


t1_get_manifest_json_file = PythonOperator(
    task_id='get_manifest_json_file',
    python_callable=get_manifest_json_file,
    provide_context=True,
    dag=dag)

t1_2 = PythonOperator(
    task_id='filter_json_list',
    python_callable=filter_json_list,
    provide_context=True,
    dag=dag)

t1_3 = PythonOperator(
    task_id='process_manifest_json_file',
    python_callable=process_manifest_json_file,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='check_valid_avro_filepath',
    python_callable=check_valid_avro_filepath,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='load_bq',
    python_callable=load_bq,
    provide_context=True,
    dag=dag

)

t4_1 = PythonOperator(
    task_id='archive_avro_files',
    python_callable=archive_processed_json_files,
    provide_context=True,
    dag=dag
)
t4_2 = PythonOperator(
    task_id='archive_corrupted_json_files',
    python_callable=archive_corrupted_json_files,
    provide_context=True,
    dag=dag
)
# t3 = BashOperator(
#     task_id='task_3',
#     bash_command=
#         "{% for i in  task_instance.xcom_pull(task_ids='manifest_read_json_files', key='return_value') %} \
#                   echo {{ i }}  {% endfor %}",
#     dag=dag
# )


t1_get_manifest_json_file >> t1_2 >> t1_3 >> t2 >> t3
t3 >> [t4_1, t4_2]
