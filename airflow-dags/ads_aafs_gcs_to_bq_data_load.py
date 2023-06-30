#Import library
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

#Config variables
ENVIRONMENT = os.getenv("env").upper()
AAFS_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')

DAG_NAME="ads_aafs_gcs_to_bq_data_load"

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
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']

#Default Argument
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
    description='AAFS data load of 4 static tables(customer_region, merch_ad_customer, merch_ad_customer_sc, merch_ad_vendor_type) and create view DDL',
    catchup=False,
    default_args=default_args,
    schedule_interval="@once",  #"@once", SCHEDULE_INTERVAL, None, "@daily" UTC = 7 PM CST on UTC's DATE-1 Daily , or set schedule_interval: "30 15 * * *"
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

def load_customer_region_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.customer_region"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/customer_region.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_merch_ad_customer_sc_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.merch_ad_customer_sc"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/merch_ad_customer_sc.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_merch_ad_customer_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.merch_ad_customer"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/merch_ad_customer.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_merch_ad_vendor_type_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.merch_ad_vendor_type"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/merch_ad_vendor_type.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

bq_create_view_task = BigQueryExecuteQueryOperator(   
    task_id='bq_create_view',
    sql='aafs/create_views.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)


load_csv_customer_region_task= PythonOperator(
    task_id='load_customer_region_file_to_bq',
    python_callable=load_customer_region_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)

load_csv_ad_customer_sc_task= PythonOperator(
    task_id='load_merch_ad_customer_sc_file_to_bq',
    python_callable=load_merch_ad_customer_sc_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_csv_ad_customer_task= PythonOperator(
    task_id='load_merch_ad_customer_file_to_bq',
    python_callable=load_merch_ad_customer_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_csv_ad_vendor_type_task= PythonOperator(
    task_id='load_merch_ad_vendor_type_file_to_bq',
    python_callable=load_merch_ad_vendor_type_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
[load_csv_customer_region_task, load_csv_ad_customer_sc_task, load_csv_ad_customer_task, load_csv_ad_vendor_type_task, bq_create_view_task]