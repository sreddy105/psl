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
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Config variables
ENVIRONMENT = os.getenv("env").upper()
AAFS_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')

DAG_NAME="ads_aafs_gcs_to_bq_data_load_prime_tables"

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

#Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        print(my_config_dict)
        return my_config_dict

#read values from yaml and set vars
my_config_values = read_parameters_from_file(AAFS_PARAMS_PATH + '/ads-aafs/aafs/config_aafs.yaml',ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'),#'2022-01-12'
    'email': FAILURE_RECIPIENT_EMAIL,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

#Labels
GCP_COST_LABELS_BIGQUERY = {
"business_unit":"ts",
"contact_email":"adserverappstatus__sabre_com",
"owner" : "sg0441328",
"name":"sab-da-ads-aafs",
"business_service": "ads",
"application_service":"aafs-load-gcp",
"environment":ENVIRONMENT.lower(),
"logical_environment":ENVIRONMENT.lower(),
"query_location":"bigquery_operator"
}

GCP_COST_LABELS_PYTHON_API = {
"business_unit":"ts",
"contact_email":"adserverappstatus__sabre_com",
"owner" : "sg0441328",
"name":"sab-da-ads-aafs",
"business_service": "ads",
"application_service":"aafs-load-gcp",
"environment":ENVIRONMENT.lower(),
"logical_environment":ENVIRONMENT.lower(),
"query_location":"python_api"
}

#Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='AAFS historical data load of 4 tables(aafs_agency_vendor_last,optin_apply_details,optin_header,optin_details)',
    catchup=False,
    default_args=default_args,
    schedule_interval=None,   #"@once", SCHEDULE_INTERVAL
    max_active_runs= 1,
    template_searchpath=AAFS_PARAMS_PATH + '/ads-aafs/aafs')

#Impersonating BigqQuery client          
def get_bq_impersonated_client(target_service_account: str, target_project: str):
                
        return bigquery.Client(project = target_project,
                                                      credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))


def getImpersonatedCredentials(target_scopes: list, target_service_account: str, target_project: str):
        from google.auth import _default
        from google.oauth2 import service_account
        source_credentials, project_id = _default.default(scopes=target_scopes)

        from google.auth import impersonated_credentials
        target_credentials = impersonated_credentials.Credentials(
        source_credentials = source_credentials,
        target_principal = f'{target_service_account}',
        target_scopes = target_scopes,
        lifetime = 600)
        return target_credentials

# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT,LAKEHOUSE_PROJECT_ID)

def load_aafs_agency_vendor_last_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.aafs_agency_vendor_last"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/aafs_agency_vendor_last.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_optin_apply_details_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.optin_apply_details"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/optin_apply_details.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_optin_header_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.optin_header"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/optin_header.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_optin_details_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.optin_details"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/optin_details.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

   
load_aafs_agency_vendor_last_task= PythonOperator(
    task_id='load_aafs_agency_vendor_last_file_to_bq',
    python_callable=load_aafs_agency_vendor_last_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)

load_optin_apply_details_task= PythonOperator(
    task_id='load_optin_apply_details_file_to_bq',
    python_callable=load_optin_apply_details_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_optin_header_task= PythonOperator(
    task_id='load_optin_header_file_to_bq',
    python_callable=load_optin_header_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_optin_details_task= PythonOperator(
    task_id='load_optin_details_file_to_bq',
    python_callable=load_optin_details_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
[load_aafs_agency_vendor_last_task, load_optin_apply_details_task, load_optin_header_task, load_optin_details_task]