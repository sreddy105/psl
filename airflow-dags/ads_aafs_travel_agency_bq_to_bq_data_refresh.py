# Import library
import os
import airflow
import re
import io
import logging
import time
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators import dummy_operator
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from zipfile import ZipFile
from zipfile import is_zipfile
from airflow.operators import dummy_operator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
from airflow import settings
from airflow.utils.helpers import chain

# Config variables
ENVIRONMENT = os.getenv("env").upper()
AAFS_PARAMS_PATH = "/home/airflow/gcs/dags"  # os.environ.get('AIRFLOW_HOME')

DAG_NAME = "ads_aafs_travel_agency_bq_to_bq_data_refresh"

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
my_config_values = read_parameters_from_file(AAFS_PARAMS_PATH + '/ads-aafs/aafs/config_aafs.yaml',
                                             ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
# INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
MAX_PCC_VENDORS = my_config_values['max_pcc_vendors']
# ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']
# STAGE_BUCKET_NAME = my_config_values['stage_bucket_name'] # 'sab-dev-dap-ads-aafs-aafs_stg'
NUMBER_OF_DAYS_TO_KEEP_PCC_REFRESH_LOAD_DATA = my_config_values['number_of_days_to_keep_pcc_refresh_load_data']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': '2022-01-12',  # datetime.strptime(START_DATE, '%Y-%m-%d'),
    'email': [FAILURE_RECIPIENT_EMAIL],  # FAILURE_RECIPIENT_EMAIL
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# Labels
GCP_COST_LABELS_BIGQUERY = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-aafs",
    "business_service": "ads",
    "application_service": "aafs-refresh-gcp",
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "bigquery_operator"
}

GCP_COST_LABELS_PYTHON_API = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-aafs",
    "business_service": "ads",
    "application_service": "aafs-refresh-gcp",
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "python_api"
}

# Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='ADS AdMedia Reporting-Agency Ad Filtering Service-Refresh GCP (AAFS Refresh) job',
    catchup=False,
    default_args=default_args,
    schedule_interval="30 12 * * *",  # SCHEDULE_INTERVAL  #None  # "30 12 * * *" (in UTC time) =  7:30 AM CST Daily 
    max_active_runs=1,
    template_searchpath=AAFS_PARAMS_PATH + '/ads-aafs/aafs')

# Impersonating BigqQuery client
def get_bq_impersonated_client(target_service_account: str, target_project: str):
    return bigquery.Client(project=target_project,
                           credentials=getImpersonatedCredentials(target_scopes, target_service_account,
                                                                  target_project))


# Impersonating gcs storage client
def get_storage_impersonated_client(target_service_account: str, target_project: str):
    from google.cloud import storage

    return storage.Client(project=target_project,
                          credentials=getImpersonatedCredentials(target_scopes, target_service_account, target_project))


# Get service account impersonated credentials
def getImpersonatedCredentials(target_scopes: list, target_service_account: str, target_project: str):
    from google.auth import _default
    from google.oauth2 import service_account
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

# Storage client with impersonated service client (between the projects)
storage_client_impersonated = get_storage_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

def get_pcc_count_query_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/R 33 count wads_aafs_pcc_refresh.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    return query

def get_load_optin_error_count_query_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/R 16 count wads_aafs_optin_error.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    # print(query)
    return query

#to get the pcc_count value from query result of sql
def get_refreshed_pccs_count():
    pcc_count = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + get_pcc_count_query_from_sql())
    pcc_refresh_count_query_result = bq_client.query(get_pcc_count_query_from_sql(), location="US", job_config=job_config)
    print("query_result.result().total_rows")
    print(pcc_refresh_count_query_result.result().total_rows)
    if pcc_refresh_count_query_result.result().total_rows == 0:
        print("No results")
    elif pcc_refresh_count_query_result.result().total_rows == 1:
        for row in pcc_refresh_count_query_result.result():
            print("refreshed pccs count=" + str(row.size))
            pcc_count = row.size
    return pcc_count

#to get the load_optin_error_count value from query result of sql
def get_load_optin_error_count():
    optin_error_count = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + get_load_optin_error_count_query_from_sql())
    optin_error_count_query_result = bq_client.query(get_load_optin_error_count_query_from_sql(), location="US",
                                                     job_config=job_config)
    print("optin_error_count_query_result.result().total_rows")
    print(optin_error_count_query_result.result().total_rows)
    if optin_error_count_query_result.result().total_rows == 0:
        print("No results")
    elif optin_error_count_query_result.result().total_rows == 1:
        for row in optin_error_count_query_result.result():
            print("optin error count=" + str(row.error_count))
            optin_error_count = row.error_count
    return optin_error_count

def refreshed_pccs_count_check():
    pcc_count = get_refreshed_pccs_count()
    if (pcc_count == 0):
        print("No PCCs were refreshed")
    elif (pcc_count > 0):
        return "apply_delete_details_for_pcc_to_optin_apply_details"
    return "send_aafs_refresh_success_mail"

def check_optin_error_count():
    optin_error_count=get_load_optin_error_count()
    if (optin_error_count == 0):
        print("No error txt")
    elif (optin_error_count>0):
        return "send_aafs_refresh_warning_mail_for_too_many_vendors"
    return "prepare_extract_data_from_aafs_agency_vendor_last"

def get_error_txt_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/R 17 select wads_aafs_optin_error.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    # print(query)
    return query

def get_error_txt():
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + get_error_txt_from_sql())
    error_txt_query_result = bq_client.query(get_error_txt_from_sql(), location="US",
                                                     job_config=job_config)
    print(error_txt_query_result)
    df = error_txt_query_result.to_dataframe()
    return df


# send warning/failure email function
def send_email(recipient_email, subject, body):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content=body, mime_charset='utf-8')


def send_success_mail():
    pcc_count = get_refreshed_pccs_count()
    email_subject = """%s Success for ads-aafs ADS:AAFS-REFRESH-GCP ads_aafs_travel_agency_bq_to_bq_data_refresh""" % (ENVIRONMENT)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """The AAFS Refresh job completed successfully. %s PCC vendors were updated.</br></br>""" % (pcc_count)
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

def send_warning_email_for_too_many_vendors():
    error_txt_df = get_error_txt()
    email_subject = """%s Warning for ads-aafs ADS:AAFS-REFRESH-GCP ads_aafs_travel_agency_bq_to_bq_data_refresh""" % (ENVIRONMENT)
    email_body = """ """
    email_body += """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """Greetings,<br /><br />"""
    email_body += """<strong>Warning:</strong> The AAFS vendor count threshold of %s has been exceeded.<br /><br />""" % (
        MAX_PCC_VENDORS)
    email_body += """<strong>PCCs for which too many vendors have been selected:</strong>"""
    email_body += """<ul>"""
    for ind in error_txt_df.index:
         email_body += """<li> %s </li>""" % (error_txt_df['error_txt'][ind])
    email_body += """</ul>"""
    email_body += """<strong>Action:</strong> Please review the PCCs above; then, send an opt-in list file that will reduce the number of vendors for each PCC above. For more information, please contact %s. <br /><br />""" % (
        WARNING_RECIPIENT_EMAIL)
    email_body += """<br>Thank you,<br />AdMedia Support<br />"""
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)


start = dummy_operator.DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    dag=dag
)

delete_data_from_staging_tables = BigQueryExecuteQueryOperator(
    task_id='delete_data_from_staging_tables',
    sql='R 01 deletes stage tables.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_region_delete_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_region_delete_rows_to_stg_refresh',
    sql='R 02 refresh region delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_region_add_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_region_add_rows_to_stg_refresh',
    sql='R 03 refresh region add.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_country_delete_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_country_delete_rows_to_stg_refresh',
    sql='R 04 refresh country delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_country_add_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_country_add_rows_to_stg_refresh',
    sql='R 05 refresh country add.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_pcc_delete_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_pcc_delete_rows_to_stg_refresh',
    sql='R 06 refresh pcc delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_pcc_add_rows_to_stg_refresh = BigQueryExecuteQueryOperator(
    task_id='insert_pcc_add_rows_to_stg_refresh',
    sql='R 07 refresh pcc add.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

insert_pcc_rows_to_pcc_refresh_load = BigQueryExecuteQueryOperator(
    task_id='insert_pcc_rows_to_pcc_refresh_load',
    sql='R 18 pcc refresh load append.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

purge_old_pcc_refresh_load_data = BigQueryExecuteQueryOperator(
    task_id='purge_old_pcc_refresh_load_data',
    sql='R 19 pcc refresh load delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "NUMBER_OF_DAYS_TO_KEEP_PCC_REFRESH_LOAD_DATA": NUMBER_OF_DAYS_TO_KEEP_PCC_REFRESH_LOAD_DATA},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

refresh_pcc_count = BigQueryExecuteQueryOperator(
    task_id='refresh_pcc_count',
    sql='R 33 count wads_aafs_pcc_refresh.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

check_refreshed_pccs_count = BranchPythonOperator(
    task_id='task_check_refreshed_pccs_count',
    python_callable=refreshed_pccs_count_check,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

apply_delete_details_for_pcc_to_optin_apply_details = BigQueryExecuteQueryOperator(
    task_id='apply_delete_details_for_pcc_to_optin_apply_details',
    sql='R 08 delete optin_apply_details from wads_aafs_pcc_refresh.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

apply_add_details_for_pcc_to_optin_apply_details = BigQueryExecuteQueryOperator(
    task_id='apply_add_details_for_pcc_to_optin_apply_details',
    sql='R 09 insert optin_apply_details from wads_aafs_pcc_refresh.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

find_last_optin_file = BigQueryExecuteQueryOperator(
    task_id='find_last_optin_file',
    sql='R 10 delete and insert wads_aafs_file_max.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

find_last_file_line = BigQueryExecuteQueryOperator(
    task_id='find_last_file_line',
    sql='R 11 delete and insert wads_aafs_line_max.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

find_last_optin_action = BigQueryExecuteQueryOperator(
    task_id='find_last_optin_action',
    sql='R 12 delete and insert wads_aafs_action_last.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

delete_all_and_insert_aafs_agency_vendor_last = BigQueryExecuteQueryOperator(
    task_id='delete_all_and_insert_aafs_agency_vendor_last',
    sql='R 13 delete and insert aafs_agency_vendor_last.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

count_optin_vendors_per_pcc = BigQueryExecuteQueryOperator(
    task_id='count_optin_vendors_per_pcc',
    sql='R 14 delete and insert wads_aafs_vendor_count.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

log_vendor_threshold_limit_errors = BigQueryExecuteQueryOperator(
    task_id='log_vendor_threshold_limit_errors',
    sql='R 15 delete and insert wads_aafs_optin_error.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID, "MAX_PCC_VENDORS": MAX_PCC_VENDORS},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

#checking for the value of optin_error_count(vendor_count_threshold_errors) & deciding the flow according to that
check_for_vendor_count_threshold_errors = BranchPythonOperator(
    task_id='check_for_vendor_count_threshold_errors',
    python_callable=check_optin_error_count,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)


prepare_extract_data_from_aafs_agency_vendor_last = BigQueryExecuteQueryOperator(
    task_id='prepare_extract_data_from_aafs_agency_vendor_last',
    sql='E2 delete and insert wads_aafs_out.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

transform_extract_data_into_wads_aafs_extract_out = BigQueryExecuteQueryOperator(
    task_id='transform_extract_data_into_wads_aafs_extract_out',
    sql='E3 delete and insert wads_aafs_extract_out.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

place_final_extract_data_into_aafs_extract_out = BigQueryExecuteQueryOperator(
    task_id='place_final_extract_data_into_aafs_extract_out',
    sql='E4 insert aafs_extract_out.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

send_aafs_refresh_success_mail = PythonOperator(
    task_id='send_aafs_refresh_success_mail',
    trigger_rule='none_failed',
    python_callable=send_success_mail,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

send_aafs_refresh_warning_mail_for_too_many_vendors = PythonOperator(
    task_id='send_aafs_refresh_warning_mail_for_too_many_vendors',
    trigger_rule='all_success',
    python_callable=send_warning_email_for_too_many_vendors,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

end = dummy_operator.DummyOperator(
    task_id='end',
    trigger_rule='none_failed',
    dag=dag
)

start >> delete_data_from_staging_tables >> insert_region_delete_rows_to_stg_refresh
# Check for any region, country, or pcc data changes in source table reference_data.mdm_travel_agency and count total changes.
insert_region_delete_rows_to_stg_refresh >> insert_region_add_rows_to_stg_refresh >> insert_country_delete_rows_to_stg_refresh >> insert_country_add_rows_to_stg_refresh
insert_country_add_rows_to_stg_refresh >> insert_pcc_delete_rows_to_stg_refresh >> insert_pcc_add_rows_to_stg_refresh >> purge_old_pcc_refresh_load_data >> insert_pcc_rows_to_pcc_refresh_load >> refresh_pcc_count >> check_refreshed_pccs_count

# When refreshed_pcc_count>0, then make the required changes to the target tables
check_refreshed_pccs_count >> apply_delete_details_for_pcc_to_optin_apply_details >> apply_add_details_for_pcc_to_optin_apply_details >> find_last_optin_file
find_last_optin_file >> find_last_file_line >> find_last_optin_action >> delete_all_and_insert_aafs_agency_vendor_last
delete_all_and_insert_aafs_agency_vendor_last >> count_optin_vendors_per_pcc

count_optin_vendors_per_pcc >> log_vendor_threshold_limit_errors >> check_for_vendor_count_threshold_errors

# When optin_error_count>0, send warning email to Dev team, since the vendor count threshold per pcc has been exceeded and needs to be fixed. Then exit the AAFS Refresh job.
check_for_vendor_count_threshold_errors >> send_aafs_refresh_warning_mail_for_too_many_vendors >> send_aafs_refresh_success_mail >> end

# When optin_error_count=0 (vendor count threshold was not exceeded), transform aafs_agency_vendor_last data and load it into final extract table: ads_aafs_extract.aafs_extract_out. Then send success email to Dev team & exit job.
check_for_vendor_count_threshold_errors >> prepare_extract_data_from_aafs_agency_vendor_last >> transform_extract_data_into_wads_aafs_extract_out >> place_final_extract_data_into_aafs_extract_out >> send_aafs_refresh_success_mail >> end

# When refreshed_pcc_count=0, the target tables do not need to be updated. Instead, send success email to Dev team and exit the AAFS Refresh job.
check_refreshed_pccs_count >> send_aafs_refresh_success_mail >> end