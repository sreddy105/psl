# ****************************************************
# STEP 1 - To import library needed for the operation
# *****************************************************
import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# ****************************************************
# STEP 2 - Default Argument
# *****************************************************
default_args = {
    'owner': 'AdMedia',
    'depends_on_past': False,
    'start_date': '2021-08-12',
    'email': ['vishnu.vijayamohanan@sabre.com'],
    'email_on_failure': False,
    'email_on_retry': False
    # 'retries': 2,
    # 'retry_delay': timedelta(minutes=2),
}
# ****************************************************
# STEP 3 - Define DAG: Set ID and assign default args and schedule interval
# *****************************************************
dag = DAG('admedia_transaction_load_vishnu',
          schedule_interval='*/5 * * * *',
          default_args=default_args
          )
# ****************************************************
# STEP 4 - # Config variables
# *****************************************************
BQ_CONN_ID = "bigquery"
BQ_PROJECT = "sab-dev-dap-common-4288"
BQ_DATASET = "admedia_transaction"
# ****************************************************
# Step 5: write down the differnt task
# *****************************************************
# task to load CSV data from cloud storage to big query table.
load_bq = GoogleCloudStorageToBigQueryOperator(
    task_id='vishnu_admedia_transaction_load',
    bucket='admedia-transaction-load',
    source_objects=['20210111_standard_feed_10306-0-32ba57b0c036ae88f8b7908d4b5fcc71-10306.avro'],
    source_format="avro",
    destination_project_dataset_table='sab-dev-dap-common-4288.admedia_transaction.test_avro_object',
    src_fmt_configs={'useAvroLogicalTypes': True},
    write_disposition='WRITE_APPEND',
    google_cloud_storage_conn_id=BQ_CONN_ID,
    bigquery_conn_id=BQ_CONN_ID,
    dag=dag
)
# ****************************************************
# Setting up Dependencies
# *****************************************************
load_bq