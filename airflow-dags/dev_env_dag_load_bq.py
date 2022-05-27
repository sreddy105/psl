from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from airflow.utils.email import send_email_smtp

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/dags/mineral-order-337219-e4d095ffa62f.json"  # do not use

"""
ENV VARIABLES
"""
BUCKET_NAME = "airflow-test-bucket-1107"
storage_client = storage.Client()
today_dt = datetime.now().date()


"""
STEP:1
"""


def scan_bucket():
    import re
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
    yesterday_dt = today_dt - timedelta(2)
    kv_feed_regex = prefix + "auction_kv_labels_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + "standard_feed_feed-11303-" + yesterday_dt.strftime("%Y%m%d") + "\w+"
    kv_feed = []
    standard_feed = []
    for blob in blobs:

        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)

    return {"kv_feed": kv_feed, "standard_feed": standard_feed}


"""
STEP:2
"""


def filter_duplicate_hours(feed_path):
    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in feed_path:
        try:
            file_split = filepath.split("-")
            key = file_split[2]
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


def remove_duplicate_hours(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='scan_bucket')

    filtered_kv_feeds = filter_duplicate_hours(feed_files['kv_feed'])

    filtered_standard_feeds = filter_duplicate_hours(feed_files['standard_feed'])

    return {"kv_feed": filtered_kv_feeds, "standard_feed": filtered_standard_feeds}


"""
STEP - 3
"""


def prepared_data_manifest_stg(feed_type):
    valid_files = feed_type["processed_files"]
    duplicate_files = feed_type["duplicate_file_list"]
    values = []

    for file_path in valid_files:
        manifest_name = file_path.split("/", 1)[-1]
        transaction_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()
        transaction_hr = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").hour
        file_valid_check = "N"
        load_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date() + timedelta(1)
        row = f"('{manifest_name}', '{transaction_dt}', '{transaction_hr}', '{file_valid_check}', '{load_dt}')"
        values.append(row)

    for file_path in duplicate_files:
        manifest_name = file_path.split("/", 1)[-1]
        transaction_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date()
        transaction_hr = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").hour
        file_valid_check = "Y"
        load_dt = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H").date() + timedelta(1)
        row = f"('{manifest_name}', '{transaction_dt}', '{transaction_hr}', '{file_valid_check}', '{load_dt}')"
        values.append(row)

    return values


def load_manifest_table(**kwargs):
    from google.cloud import bigquery

    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')

    insert_job_kv_feeds_values = prepared_data_manifest_stg(feed_files['kv_feed'])
    insert_kv_feeds_sql = "INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES " + ",".join(
        insert_job_kv_feeds_values)
    print(insert_kv_feeds_sql)

    insert_job_kv_feeds_values = prepared_data_manifest_stg(feed_files['standard_feed'])
    insert_kv_feeds_sql = "INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES " + ",".join(
        insert_job_kv_feeds_values)
    print(insert_kv_feeds_sql)

    # insert_client = bigquery.Client()
    # table_id_status = ""
    # job_config = bigquery.QueryJobConfig(labels={})
    # insert_job = insert_client.query("INSERT INTO `mineral-order-337219.test_dags.dummy_table` VALUES ('1', '2')",
    #                                  location="US", job_config=job_config)
    # insert_job.result()


"""
STEP-4
"""


def move_file(bucket_name: str, destination_bucket_name: str, source_files: list, destination_files: list):
    """Moves a blob from one bucket to another with a new name."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"

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


def archive_duplicate_files(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')

    destination_files_common_path = "manifest/duplicates/"
    bucket_name = "airflow-test-bucket-1107"
    destination_bucket_name = "airflow-test-bucket-1107"

    duplicate_files_kv_feed = feed_files['kv_feed']['duplicate_file_list']
    duplicate_files_standard_feed = feed_files['standard_feed']['duplicate_file_list']

    destination_path_duplicate_kv_feeds = [destination_files_common_path + file.split("/")[-1] for file in
                                           duplicate_files_kv_feed]

    destination_path_duplicate_standard_feeds = [destination_files_common_path + file.split("/")[-1] for file in
                                                 duplicate_files_standard_feed]

    move_file(bucket_name, destination_bucket_name,
              source_files=duplicate_files_kv_feed,
              destination_files=destination_path_duplicate_kv_feeds)

    move_file(bucket_name, destination_bucket_name,
              duplicate_files_standard_feed,
              destination_files=destination_path_duplicate_standard_feeds)

    return True


# Archieving Valid Manifest files
def archive_valid_files(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')

    bucket_name = "airflow-test-bucket-1107"
    destination_bucket_name = "airflow-test-bucket-1107"

    valid_files_kv_feed = feed_files['kv_feed']['processed_files']
    valid_files_standard_feed = feed_files['standard_feed']['processed_files']

    move_file(bucket_name, destination_bucket_name,
              source_files=valid_files_kv_feed,
              destination_files=valid_files_kv_feed)

    move_file(bucket_name, destination_bucket_name,
              source_files=valid_files_standard_feed,
              destination_files=valid_files_standard_feed)

    return True


# archive avro files after loading data
def archive_valid_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    kv_feeds_avro_files = xComm_var.xcom_pull(key='kv_feed_avro_file', task_ids='download_json_contents')
    standard_feeds_avro_files = xComm_var.xcom_pull(key='standard_feed_avro_file', task_ids='download_json_contents')
    all_avro_files_path = kv_feeds_avro_files + standard_feeds_avro_files

    bucket_name = "airflow-test-bucket-1107"
    destination_bucket_name = "airflow-test-bucket-1107"

    move_file(bucket_name, destination_bucket_name,
              source_files=all_avro_files_path,
              destination_files=all_avro_files_path)



"""
Send email

"""
#this will in your global variable
email_recipient_list = "abc@email.com;test@email.com"


def send_warning_email(recipient_email, subject, body):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content = body)

def check_history_file(**kwargs):
    if history_files_list:
        history_files_warning_email(environment="Dev", email_type="Warning", history_files_list, dag_name="test_1")

def history_files_warning_email(environment, email_type, history_files_list, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = f"{environment} {email_type} for {product} {assets_tag} {dag_name}"
    email_body = "<h2><p style='color:#FF0000'>***This email has been generated automatically - DO NOT REPLY Directly ***</p></h2>"
    email_body = "<strong>Warning</strong>: Historical APN manifest files were received today containing data prior to yesterday</br></br>"
    email_body +="<strong>Filenames:</strong></br></br>"
    email_body +="<ul>"
    for filename in history_files_list:
        email_body += f"<li>{filename}</li>"
    email_body += "</ul></br></br>"

    email_body += "<strong>Please validate Historical file(s) and if needed, requeset operations to run " \
                  "adhoc DAG for historical file(s). </strong>"

    send_warning_email(recipent_email="", subject=email_subject, body=email_body)


def missing_avro_files_failure_email(environment, email_type, missing_avro_files, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = f"{environment}\t {email_type}\t for {product}\t {assets_tag}\t {dag_name}"
    email_body = "<h2><p style='color:#FF0000'>***This email has been generated automatically - DO NOT REPLY Directly ***</p></h2>"
    email_body = "<strong>Warning</strong>: The following avro file(s) for transaction load processing is/are missing</br></br>"
    email_body +="<strong>Filenames:</strong></br></br>"
    email_body +="<ul>"
    for filename in missing_avro_files:
        email_body += f"<li>{filename}</li>"
    email_body += "</ul></br></br>"

    email_body += "<strong>Action: </strong> Request AppNexus to send missing avro files"

    send_warning_email(recipent_email="", subject=email_subject, body=email_body)



def duplicate_hours_files_email(environment, email_type, duplicate_hour_files, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = f"{environment}\t {email_type}\t for {product}\t {assets_tag}\t {dag_name}"
    email_body = "<h2><p style='color:#FF0000'>***This email has been generated automatically - DO NOT REPLY Directly ***</p></h2>"
    email_body = "<strong>Warning</strong>: More than one manifest was received today for particular hour(s) shown in filenames below; " \
                 "only the latest file for each hour was processed today </br></br>"
    email_body += "<strong>Duplicate Filename(s), which were not processed:</strong></br></br>"
    email_body += "<ul>"
    for filename in duplicate_hour_files:
        email_body += f"<li>{filename}</li>"
    email_body += "</ul></br></br>"

    email_body += f"<strong>Action: </strong> Find duplicate file(s) in lakehouse bucket folder path: " \
                  f"sab-{environment}-dap-promospots-ads-transaction_archival/manifests/duplicates."

    send_warning_email(recipent_email="", subject=email_subject, body=email_body)

"""
STEP-5
"""


def download_avro_files_path(bucket_name, blob_name):
    """Downloads a blob into memory."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # blob_name = "storage-object-name"
    import json

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    json_file_contents = json.loads(blob.download_as_string())
    path = json_file_contents['path'][1:]
    avro_files = [path + "/" + filepath['name'] for filepath in json_file_contents['files']]
    return avro_files


def download_json_contents(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')
    valid_kv_feeds_json = feed_files['kv_feed']['processed_files']
    valid_standard_feeds_json = feed_files['standard_feed']['processed_files']

    kv_feed_avro_file = []
    standard_feed_avro_file = []
    for file in valid_kv_feeds_json:
        kv_feed_avro_file.extend(download_avro_files_path(bucket_name="", blob_name=file))

    for file in valid_standard_feeds_json:
        standard_feed_avro_file.extend(download_avro_files_path(bucket_name="", blob_name=file))

    print(kv_feed_avro_file, standard_feed_avro_file)
    xComm_var.xcom_push(key='kv_feed_avro_file', value=kv_feed_avro_file)
    xComm_var.xcom_push(key="standard_feed_avro_file", value=standard_feed_avro_file)
    return True


"""
STEP: 6
"""


def load_kv_feed_avro_file(avro_files_path):
    from google.cloud import bigquery

    client = bigquery.Client()

    table_id = ""

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("auction_id_64", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("date_time", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("value", "STRING", mode="REQUIRED")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.AVRO,
    )
    common_path = f"gs://{BUCKET_NAME}"
    gs_path = [common_path + path for path in avro_files_path]

    uri = gs_path

    load_job = client.load_table_from_file(uri, table_id, job_config=job_config)

    load_job.result()

    destination_table = client.get_table(table_id)
    print("Loaded {} rows to table {}.{}".format(destination_table.num_rows, destination_table.dataset_id,
                                                 destination_table.table_id))


def load_avro_files_to_bq(**kwargs):
    xComm_var = kwargs['ti']
    # standard_feeds_avro_files = xComm_var.xcom_pull(key='standard_feed_avro_file', task_ids=['download_json_contents'])
    # load_standard_feed_avro_file(standard_feeds_avro_files)
    common_path = f"gs://{BUCKET_NAME}"
    gs_path = []
    kv_feeds_avro_files = xComm_var.xcom_pull(key='kv_feed_avro_file', task_ids=['download_json_contents'])

    load_kv_feed_avro_file([common_path + path for path in kv_feeds_avro_files])


"""
CHECK VALID AVRO FILES:
"""


def is_path_exists(bucket_name, files_list):
    from airflow import AirflowException
    for path in files_list:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            raise AirflowException("FILE NOT FOUND:", path)

    return True


def check_valid_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    kv_feeds_avro_files = xComm_var.xcom_pull(key='kv_feed_avro_file', task_ids='download_json_contents')
    standard_feeds_avro_files = xComm_var.xcom_pull(key='standard_feed_avro_file', task_ids='download_json_contents')
    files_list = kv_feeds_avro_files + standard_feeds_avro_files
    is_path_exists(bucket_name=BUCKET_NAME, files_list=files_list)


"""
TODOs:Below
"""


def test_1_push(**kwargs):
    xComm_var = kwargs['ti']
    list_1 = []
    for i in range(0, 100):
        list_1.append(str(i))
    xComm_var.xcom_push(key='test_1', value=list_1)


def test_2_pull(**kwargs):
    xComm_var = kwargs['ti']
    kv_feeds_avro_files = xComm_var.xcom_pull(key='test_1', task_ids=['test_1_push'])
    print(kv_feeds_avro_files)
    for i in kv_feeds_avro_files:
        print(str(i) + '1')


def check_older_files(**kwargs):
    pass


def blob_exists(projectname, credentials, bucket_name, filename):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    return blob.exists()


def check_valid_avro_files(*kwargs):
    pass


default_args = {
    "owner": "dhruv-airflow-test-dev",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("hook_gcp_bucket_final_dev", default_args=default_args, schedule_interval=None)

# scan_bucket_t1_1 = PythonOperator(
#     task_id='scan_bucket',
#     python_callable=scan_bucket,
#     provide_context=True,
#     dag=dag)
#
# duplicate_hour_check_t2_1 = PythonOperator(
#     task_id='remove_duplicate_hours',
#     python_callable=remove_duplicate_hours,
#     provide_context=True,
#     dag=dag)
#
# load_manifest_table_t3_1 = PythonOperator(
#     task_id='load_manifest_table',
#     python_callable=load_manifest_table,
#     provide_context=True,
#     dag=dag)
#
# archive_duplicate_files_t4_1 = PythonOperator(
#     task_id='archive_duplicate_files',
#     python_callable=archive_duplicate_files,
#     provide_context=True,
#     dag=dag)
#
# scan_bucket_t1_1 >> duplicate_hour_check_t2_1
# duplicate_hour_check_t2_1 >> [load_manifest_table_t3_1, archive_duplicate_files_t4_1]

archive_duplicate_files_t4_1 = PythonOperator(
    task_id='test_1_push',
    python_callable=test_1_push,
    provide_context=True,
    dag=dag)

archive_duplicate_files_t5_1 = PythonOperator(
    task_id='test_2_pull',
    python_callable=test_2_pull,
    provide_context=True,
    dag=dag)

archive_duplicate_files_t4_1 >> archive_duplicate_files_t5_1

""" 
PART - 2
"""


def check_history_files():
    prefix = "manifest/"
    blobs = storage_client.list_blobs(BUCKET_NAME, prefix=prefix, delimiter="/")
    yesterdays_dt = datetime.now().date() - timedelta(1)
    history_files_list = []
    for blob in blobs:
        file_split = blob.name.split("-")

        file_dt = datetime.strptime(file_split.split("-")[2], "%Y%m%d%H").date()
        file_ts = datetime.strptime(file_split.split("-")[3], "%Y%m%d%H%M%S").date()
        if file_dt < yesterdays_dt and (yesterdays_dt <= file_ts <= today_dt):
            print("found history file:{}".format(blob.name))
            history_files_list.append(blob.name)
    if history_files_list:
        send_warning_email(body = "||".join(history_files_list))



