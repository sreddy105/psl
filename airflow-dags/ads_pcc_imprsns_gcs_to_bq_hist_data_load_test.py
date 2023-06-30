# Import library
import os
import airflow
import re
import logging
import time
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators import dummy_operator
from google.cloud import bigquery
from airflow.operators.python_operator import PythonOperator

# Config variables
ENVIRONMENT = os.getenv("env").upper()
REFERENCE_PARAMS_PATH = "/home/airflow/gcs/dags/ads-pcc-imprsns-test"

DAG_NAME="ads_pcc_imprsns_gcs_to_bq_hist_data_load_test"

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

# Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        print(my_config_dict)
        return my_config_dict


# read values from yaml and set vars
my_config_values = read_parameters_from_file(REFERENCE_PARAMS_PATH + '/pcc-imprsns/config_pcc_imprsns_test.yaml',
                                             ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
# INGRESS_PROJECT_ID = my_config_values['ingress_project_id']
EGRESS_PROJECT_ID = my_config_values['egress_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
SERVICE_ACCOUNT = my_config_values['service_account']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
# INGRESS_EFG_BUCKET_NAME = my_config_values['ingress_efg_bucket_name']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': '2022-01-12',
    'email': FAILURE_RECIPIENT_EMAIL,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

GCP_COST_LABELS_PYTHON_API = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-pcc-imprsns",   # sab-da-(product_name)
    "business_service": "ads",  # This is business service asset tag, so will always be "ads" for AdMedia.
    "application_service": "pcc-impressions-extract-gcp",  # This is technical service asset tag.
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "python_api"
}

# Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='ADS AdMedia Reporting-PCC Impressions GCP - Audit History job',  # Technical service name.
    catchup=False,
    default_args=default_args,
    schedule_interval="@once", #'0 5 * * *',  #None # SCHEDULE_INTERVAL
    max_active_runs=1,
    template_searchpath=REFERENCE_PARAMS_PATH + '/pcc-imprsns')

# Impersonating BigQuery client
def get_bq_impersonated_client(target_service_account: str, target_project: str):
    return bigquery.Client(project=target_project,
                           credentials=getImpersonatedCredentials(target_scopes, target_service_account,
                                                                  target_project))

# Get service account impersonated credentials
def getImpersonatedCredentials(target_scopes: list, target_service_account: str, target_project: str):
    from google.auth import _default
    source_credentials, project_id = _default.default(scopes=target_scopes)

    from google.auth import impersonated_credentials
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=f'{target_service_account}',
        target_scopes=target_scopes,
        lifetime=600)
    return target_credentials

# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT, LAKEHOUSE_PROJECT_ID)

def load_pcc_impressions_extract_audit_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.pcc_impressions_extract_audit"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        field_delimiter=",",
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/pcc_impressions_extract_audit.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    
    print("Loaded {} rows.".format(load_job.output_rows))
    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Total table {} rows.".format(destination_table.num_rows))
    
    
load_pcc_impressions_extract_audit_file_to_bq_task= PythonOperator(
    task_id='load_pcc_impressions_extract_audit_file_to_bq',
    python_callable=load_pcc_impressions_extract_audit_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)

load_pcc_impressions_extract_audit_file_to_bq_task