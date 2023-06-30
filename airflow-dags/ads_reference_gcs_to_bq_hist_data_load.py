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
REFERENCE_PARAMS_PATH = "/home/airflow/gcs/dags/ads-reference"

DAG_NAME="ads_reference_gcs_to_bq_hist_data_load"

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
my_config_values = read_parameters_from_file(REFERENCE_PARAMS_PATH + '/reference/config_reference.yaml',ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
INGRESS_PROJECT_ID = my_config_values['ingress_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
INGRESS_EFG_BUCKET_NAME = my_config_values['ingress_efg_bucket_name']
PIPELINE_BUCKET_NAME = my_config_values['pipeline_bucket_name']
ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'), #'2022-01-12',
    'email': FAILURE_RECIPIENT_EMAIL,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Labels
GCP_COST_LABELS_BIGQUERY = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-reference",
    "business_service": "ads",  # This is business service asset tag, so will always be "ads" for AdMedia.
    "application_service": "reference-load-gcp",  # This is technical service asset tag.
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "bigquery_operator"
}

GCP_COST_LABELS_PYTHON_API = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-reference",
    "business_service": "ads",  # This is business service asset tag, so will always be "ads" for AdMedia.
    "application_service": "reference-load-gcp",  # This is technical service asset tag.
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "python_api"
}

# Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='ADS AdMedia Reporting-Reference History Load GCP job',  # Technical service name.
    catchup=False,
    default_args=default_args,
    schedule_interval="@once", # SCHEDULE_INTERVAL, None, "@once"
    max_active_runs=1,
    template_searchpath=REFERENCE_PARAMS_PATH + '/reference')

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

def load_advertiser_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.advertiser"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/advertiser.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_campaign_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.campaign"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/campaign.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_campaign_targeting_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.campaign_targeting"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/campaign_targeting.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_categories_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.categories"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/categories.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
    
def load_creative_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.creative"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/creative.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_insertion_order_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.insertion_order"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/insertion_order.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_searchterm_targeting_vars_order_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.searchterm_targeting_vars"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/searchterm_targeting_vars.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def load_site_file_to_bq():
    table_id = f"{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.site"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        field_delimiter="^",
        # The source format defaults to CSV, so the line below is optional.
        #source_format=bigquery.SourceFormat.csv,
    )
    uri = f"gs://{INGRESS_BUCKET_NAME}/site.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))
   
load_advertiser_file_to_bq_task= PythonOperator(
    task_id='load_advertiser_file_to_bq',
    python_callable=load_advertiser_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)

load_campaign_file_to_bq_task= PythonOperator(
    task_id='load_campaign_file_to_bq',
    python_callable=load_campaign_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_campaign_targeting_file_to_bq_task= PythonOperator(
    task_id='load_campaign_targeting_file_to_bq',
    python_callable=load_campaign_targeting_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_categories_file_to_bq_task= PythonOperator(
    task_id='load_categories_file_to_bq',
    python_callable=load_categories_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_creative_file_to_bq_task= PythonOperator(
    task_id='load_creative_file_to_bq',
    python_callable=load_creative_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_insertion_order_file_to_bq_task= PythonOperator(
    task_id='load_insertion_order_file_to_bq',
    python_callable=load_insertion_order_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_searchterm_targeting_vars_order_file_to_bq_task= PythonOperator(
    task_id='load_searchterm_targeting_vars_order_file_to_bq',
    python_callable=load_searchterm_targeting_vars_order_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
load_site_file_to_bq_task= PythonOperator(
    task_id='load_site_file_to_bq',
    python_callable=load_site_file_to_bq,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)
    
[load_advertiser_file_to_bq_task, load_campaign_file_to_bq_task, load_campaign_targeting_file_to_bq_task, load_categories_file_to_bq_task, load_creative_file_to_bq_task, load_insertion_order_file_to_bq_task, load_searchterm_targeting_vars_order_file_to_bq_task, load_site_file_to_bq_task]