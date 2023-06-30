# Import library
import os
import airflow
import re
import json
import logging
import requests
import time
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators import dummy_operator
from google.cloud import storage
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.exceptions import AirflowSkipException
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from google.cloud import bigquery

# Config variables
ENVIRONMENT = os.getenv("env").upper()
TRANSACTION_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')

#Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        print(my_config_dict)
        return my_config_dict

#read values from yaml and set vars
my_config_values = read_parameters_from_file(TRANSACTION_PARAMS_PATH + '/promospots-ads-test/transaction/config.yaml',ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']
KV_FEED_PREFIX = my_config_values['kv_feed_prefix']
STD_FEED_PREFIX = my_config_values['std_feed_prefix']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
GOVERNANCE_PROJECT_ID = my_config_values['governance_project_id']

load_ts = datetime.now()

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

DAG_NAME="admedia_gcs_to_bq_avro_data_load_test"

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'),
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
"name":"sab-da-promospots-ads",
"business_service": "ads",
"application_service":"transaction-load",
"environment":ENVIRONMENT.lower(),
"logical_environment":ENVIRONMENT.lower(),
"query_location":"bigquery_operator"
}

GCP_COST_LABELS_PYTHON_API = {
"business_unit":"ts",
"contact_email":"adserverappstatus__sabre_com",
"owner" : "sg0441328",
"name":"sab-da-promospots-ads",
"business_service": "ads",
"application_service":"transaction-load",
"environment":ENVIRONMENT.lower(),
"logical_environment":ENVIRONMENT.lower(),
"query_location":"python_api"
}

#Define DAG
dag = DAG(
    dag_id=DAG_NAME,
    description='ADS AdMedia Reporting-Transaction Load job',
    catchup=False,
    default_args=default_args,
    schedule_interval=None,
    max_active_runs= 1,
    template_searchpath=TRANSACTION_PARAMS_PATH + '/promospots-ads/transaction')

#Impersonating BigqQuery client          
def get_bq_impersonated_client(target_service_account: str, target_project: str):
                
        return bigquery.Client(project = target_project,
                                                      credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))

#Impersonating gcs storage client                                                       
def get_storage_impersonated_client(target_service_account: str, target_project: str):
        from google.cloud import storage
        
        return storage.Client(project = target_project,
                                                      credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))    

#Impersonating secret manager credentials
def get_sm_impersonated_client(target_service_account: str, target_project: str):
         # Import the Secret Manager client library.
        from google.cloud import secretmanager
        
        return secretmanager.SecretManagerServiceClient(credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))

#Get API Hub service account impersonated credentials
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

# Storage client with impersonated service client (between the projects)
storage_client_impersonated = get_storage_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT,PIPELINE_PROJECT_ID)

# send warning/failure email function
def send_email(recipient_email, subject, body):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content = body, mime_charset='utf-8')

def api_warning_email(environment, email_type, missing_avro_files, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: The API call to AppNexus to obtain the total number of impressions for today failed.  The count of 0 (zero) will be placed in the promospots_ads.transaction_audit_log.apn_impression_count column.</br></br>"""
    email_body += """<strong>Action</strong>: </strong> Once AppNexus API reporting issue is resolved, manually obtain the daily impression total via Postman call; then, submit change request to update the promospots_ads.transaction_audit_log.apn_impression_count column, so it will then contain the correct total."""
    
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)    

def missing_avro_files_warning_email(environment, email_type, missing_avro_files, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: The following avro file(s) for transaction load processing is/are missing.</br></br>"""
    email_body += """<strong>Filenames:</strong>"""
    email_body += """<ul>"""
    for filename in missing_avro_files:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""

    email_body += """<strong>Action</strong>: </strong> Request AppNexus to send missing avro files."""

    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def duplicate_hours_files_warning_email(environment, email_type, duplicate_hour_files, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: More than one manifest file was received today for particular hour(s) shown in filenames below; only the latest file for each hour was processed today. </br></br>"""
    email_body += """<strong>Duplicate Filename(s), which were not processed:</strong>"""
    email_body += """<ul>"""
    for filename in duplicate_hour_files:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""

    email_body += """<strong>Action: </strong> Find duplicate file(s) in lakehouse bucket folder path: sab-[env]-dap-promospots-ads-transaction_archival/manifests/duplicates."""

    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def history_files_warning_email(environment, email_type, history_files_list, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: Historical APN manifest files were received today containing data prior to yesterday.</br></br>"""
    email_body += """<strong>Filenames:</strong>"""
    email_body += """<ul>"""
    for filename in history_files_list:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""

    email_body += """<strong>Action</strong>: None needed. The daily DAG contains logic to check for historical files and process them."""

    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def missing_manifest_failure_email_job_does_not_fail(environment, email_type, missing_manifest_files_count, transaction_dt, dag_name,product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>: The process detected only %s out of 48 valid manifest files for transaction_dt %s; 48 manifest files (1 standard file and 1 auction kv file for each hour of the day) are expected daily.  The files that were received will be processed today.</br></br>""" %(str(missing_manifest_files_count), str(transaction_dt))
    email_body += """<strong>Actions:</strong>"""
    email_body += """<ul>
                        <li> Run this SQL to determine which manifest files are missing:
                            <ul>
                                <li>SELECT * FROM `%s.%s.manifest_log_stg_copy` WHERE transaction_dt IN ( 
    SELECT MAX(transaction_dt) FROM `%s.%s.manifest_log_stg_copy` WHERE transaction_dt != ( 
    SELECT MAX(transaction_dt) FROM `%s.%s.manifest_log_stg_copy`)) 
 AND duplicate_manifest_ind = 'N' AND processed_flag = 'N' ORDER BY transaction_hour asc;</li>
                                <li>NOTE:  This table is cleared at the beginning of each job; if this check is not done prior to the next daily run, then the missing files will need to be determined by checking the archived files in GCP lakehouse bucket:  sab-%s-dap-promospots-ads-transaction_archival/manifests.</li>
                            </ul>
                        </li>
                        <li>Create Xandr ticket for AppNexus to send the missing files immediately.
                            <ul>
                                <li>Open the Transaction Load Runbook:  <a href='%s'>KB0036207</a></li>
                                <li>Inside the runbook, in section 3.4 “Special Monitoring Requests”, find instructions to open the ticket with AppNexus.</li>
                            </ul>
                        </li>
                        <li>FYI - These same instructions are also in the E2E document attached to the top of KB0036207 (in section 5.2.1 “AppNexus / Xandr ticket process”).</li>
                    </ul>
    """ %(PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID,PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID,PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID,environment.lower(), 'https://sabre.service-now.com/nav_to.do?uri=%2Fkb_view.do%3Fsysparm_article%3DKB0036207')
    send_email(recipient_email=FAILURE_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def missing_transaction_dt_for_ctl_table_or_ingress_failure_email(environment, email_type, dag_name,product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>: The process detected NO valid manifest files; 48 manifest files (1 standard file and 1 auction kv file for each hour of the day) are expected daily.</br></br>"""
    email_body += """<strong>Actions:</strong>"""
    email_body += """<ul>
                        <li> Create Xandr ticket for AppNexus to send the missing files immediately.
                            <ul>
                                <li>Open the Transaction Load Runbook:  <a href='https://sabre.service-now.com/nav_to.do?uri=%2Fkb_view.do%3Fsysparm_article%3DKB0036207'>KB0036207</a></li>
                                <li>Inside the runbook, in section 3.4 “Special Monitoring Requests”, find instructions to open the ticket with AppNexus.</li>
                            </ul>
                        </li>
                        <li>FYI - These same instructions are also in the E2E document attached to the top of KB0036207 (in section 5.2.1 “AppNexus / Xandr ticket process”).</li>
                     </ul>
    """
    send_email(recipient_email=FAILURE_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def missing_avro_feed_failure_email(environment, email_type, dag_name,product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>: The process detected NO standard and auction kv feed avro files that were included in the corresponding manifest files.</br></br>"""
    email_body += """<strong>Actions:</strong>"""
    email_body += """<ul>
                        <li> Create Xandr ticket for AppNexus to send the missing files immediately.
                            <ul>
                                <li>Open the Transaction Load Runbook:  <a href='https://sabre.service-now.com/nav_to.do?uri=%2Fkb_view.do%3Fsysparm_article%3DKB0036207'>KB0036207</a></li>
                                <li>Inside the runbook, in section 3.4 “Special Monitoring Requests”, find instructions to open the ticket with AppNexus.</li>
                            </ul>
                        </li>
                        <li>FYI - These same instructions are also in the E2E document attached to the top of KB0036207 (in section 5.2.1 “AppNexus / Xandr ticket process”).</li>
                     </ul>
    """
    send_email(recipient_email=FAILURE_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

def skip_exception_for_missing_archive_manifest_files_warning_email(environment, email_type, missing_avro_files, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: No files exist in archive bucket, so this job will skip the tasks to retrieve files from archive and, instead, check for files in the ingress bucket.</br></br>"""
    
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)
    
def dag_trigger_warning_email(dag_name, environment, email_type, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<b>FYI Only: No action is required.</b></br></br>"""
    email_body += """This daily AdMedia DAG, ads_gcs_to_bq_avro_data_load with asset tag ADS:TRANSACTION-LOAD, has detected the existence of historical file(s) in its ingress/egress bucket, which also need to be processed.  As a result, the last step of this DAG will trigger another instance of this same DAG to start <i><u>after</u></i> this DAG has finished; the new instance will process the additional historical source file(s).</br></br>"""
    email_body += """<strong>Note</strong>: In this job’s DAG description section, the “max_active_runs” option is set to 1 to enforce that only one instance of the DAG will run at a time.</br></br>"""
    
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# Generic function to execute SQL query in BigQuery database
def execute_query(client, query):
    query_job = client.query(query)
    results = query_job.result()
    print("EXECUTE_QUERY_RESULTS:",results) 
    return results

# Update control table function for the history transaction_dt    
def change_control_table(bq_client, transaction_dt, processed_flag, historical_file_flag):
    dml_statement = (
        "UPDATE `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` \
        SET processed_flag = '{processed_flag}', historical_file_flag = '{historical_file_flag}', load_ts = '{load_ts}' \
        WHERE transaction_dt = '{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt,processed_flag=processed_flag,historical_file_flag=historical_file_flag,load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
        
    execute_query(bq_client, dml_statement)

# Filter duplicate hours for all manifest files in the ingress bucket
def filter_duplicate_hours(feed_path):
    filter_hours_dict = {}
    duplicate_file_list = []

    for filepath in feed_path:
        try:
            file_split = filepath.split("-")
            key = file_split[0] + file_split[2]
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

# Scan all manifest files in ingress bucket
def scan_bucket_manifest_files():
    rows_to_insert = []
    prefix = "manifests/"
    blobs = storage_client_impersonated.list_blobs(INGRESS_BUCKET_NAME, prefix=prefix, delimiter="/")
    blobs_list = [str(blob.name) for blob in blobs]
    filtered_files = filter_duplicate_hours(blobs_list)
    values_fmt_str = "('{manifest_name}', '{transaction_dt}', {transaction_hour}, '{duplicate_manifest_ind}', '{processed_flag}', '{load_ts}')"
    print("Files LIst:",filtered_files, "BolbFiles:", blobs_list)
    for file_path in filtered_files['processed_files']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hour = parsed_date_stamp.hour
        duplicate_manifest_ind = "N"
        processed_flag = "N"
        rows_to_insert.append(values_fmt_str.format(manifest_name=manifest_name,
                                                  transaction_dt=datetime.strftime(transaction_dt,'%Y-%m-%d'),
                                                  transaction_hour=transaction_hour,
                                                  duplicate_manifest_ind=duplicate_manifest_ind,
                                                  processed_flag=processed_flag,
                                                  load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
    for file_path in filtered_files['duplicate_file_list']:
        manifest_name = file_path.split("/", 1)[-1]
        parsed_date_stamp = datetime.strptime(file_path.split("-")[2], "%Y%m%d%H")
        transaction_dt = parsed_date_stamp.date()
        transaction_hour = parsed_date_stamp.hour
        duplicate_manifest_ind = "Y"
        processed_flag = "N"
        rows_to_insert.append(values_fmt_str.format(manifest_name=manifest_name,
                                                  transaction_dt=datetime.strftime(transaction_dt,'%Y-%m-%d'),
                                                  transaction_hour=transaction_hour,
                                                  duplicate_manifest_ind=duplicate_manifest_ind,
                                                  processed_flag=processed_flag,
                                                  load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))

    return rows_to_insert

# Load scanned manifest files into manifest_log_stg table
def load_manifest_staging_table():
    rows = scan_bucket_manifest_files()
    insert_sql_query = "INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` VALUES ".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    insert_sql_query += ",".join(rows)

    execute_query(bq_client, insert_sql_query)

# Checking whether transaction_dt is history date or not
def is_history_date(bq_client, transaction_dt):
    query = "SELECT transaction_dt, processed_flag from `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` " \
            "WHERE transaction_dt = '{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt)
    query_job = bq_client.query(query)
    results = query_job.result()
    if results.total_rows != 0:
        print("date Exists")
        return True
    return False

# Checking history files and notifying through warning email
def notify_history_files_list(bq_client, transaction_dt):
    query = "SELECT manifest_name FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` " \
            "WHERE transaction_dt='{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt)
    
    history_files_list = []    
    results = execute_query(bq_client, query)
        
    for row in results:
        history_files_list.append("{}".format(row.manifest_name))
    if history_files_list:
        history_files_warning_email(environment=ENVIRONMENT, email_type="Warning", history_files_list=history_files_list, dag_name=DAG_NAME)

# Analyze the manifest_log_stg table and load the second MAX transaction_dt into control table (ctl_transaction)
def load_control_table():
    rows_insert_for_control_table = []
    query = "SELECT count(manifest_name) as manifest_files_count, transaction_dt \
    FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
    WHERE transaction_dt IN (SELECT MAX(transaction_dt) \
        FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
        WHERE duplicate_manifest_ind = 'N' AND processed_flag = 'N' \
        AND transaction_dt < (SELECT MAX(transaction_dt) \
            FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
            WHERE duplicate_manifest_ind = 'N' AND processed_flag = 'N')) \
    AND duplicate_manifest_ind = 'N' AND processed_flag = 'N' \
    GROUP BY transaction_dt".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
        
    print("SELECT_QUERY:",query)
    results = execute_query(bq_client, query)
    print("SELECT_COUNT:",results)
    insert_query = "INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` VALUES ".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    values_fmt_str = "('{transaction_dt}', '{processed_flag}', '{historical_file_flag}', '{load_ts}')"
    if results.total_rows == 0:
        missing_transaction_dt_for_ctl_table_or_ingress_failure_email(dag_name=DAG_NAME, environment=ENVIRONMENT, email_type="Failure")
        raise Exception("No manifest files exist to process")
    for result in results:
        if result.manifest_files_count >= 48:
            rows_insert_for_control_table.append(values_fmt_str.format(transaction_dt=datetime.strftime(result.transaction_dt,'%Y-%m-%d'), processed_flag="N", historical_file_flag="N", load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
            print("INSERT_QUERY:",rows_insert_for_control_table)
        elif is_history_date(bq_client, result.transaction_dt):
            # update the processed flag to 'N' to again process the history files, and also update the historical_file_flag to 'Y'
            change_control_table(bq_client, result.transaction_dt, 'N', 'Y')
            # email the history files received
            notify_history_files_list(bq_client, result.transaction_dt)
        else:
            # lessthan 48 files for the transaction_dt being processed 
            print("MISSING_FILES_COUNT:", int(result.manifest_files_count))
            rows_insert_for_control_table.append(values_fmt_str.format(transaction_dt=datetime.strftime(result.transaction_dt,'%Y-%m-%d'), processed_flag="N", historical_file_flag="N", load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
            missing_manifest_failure_email_job_does_not_fail(missing_manifest_files_count=int(result.manifest_files_count), transaction_dt=result.transaction_dt, dag_name=DAG_NAME, environment=ENVIRONMENT, email_type="Failure")
    
    # insert new transaction dates into control table
    if rows_insert_for_control_table:
        insert_query += ",".join(rows_insert_for_control_table)
        execute_query(bq_client, insert_query)

# Getting transaction_dt from control table (ctl_transaction)
def get_transaction_dt():
    select_query = "SELECT MAX(transaction_dt) transaction_dt FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` \
    WHERE processed_flag='N'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)

    results = execute_query(bq_client, select_query)
    for result in results:
        return result.transaction_dt

# Generic function to move file from source bucket to target bucket
def move_file(bucket_name: str, destination_bucket_name: str, source_files: list, destination_files: list, delete = None):
    source_bucket = storage_client_impersonated.bucket(bucket_name)
    destination_bucket = storage_client_impersonated.bucket(destination_bucket_name)
    for source_path, destination_path in zip(source_files, destination_files):
        source_blob = source_bucket.blob(source_path)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_path
        )
        if delete is not None:
            source_bucket.delete_blob(source_path)

        print(
            "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

# Copy manifest files from archival bucket
def fetch_manifest_files_from_archival(**kwargs):
    prefix = "manifests/"
    blobs = storage_client_impersonated.list_blobs(ARCHIVE_BUCKET_NAME, prefix=prefix, delimiter="/")
    kv_feed = []
    standard_feed = []
    transaction_dt = get_transaction_dt()    
    
    kv_feed_regex = prefix + KV_FEED_PREFIX + transaction_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + STD_FEED_PREFIX + transaction_dt.strftime("%Y%m%d") + "\w+"
    print(kv_feed_regex,standard_feed_regex)
    for blob in blobs:
        
        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)
    if not (kv_feed and standard_feed):
        skip_exception_for_missing_archive_manifest_files_warning_email(environment=ENVIRONMENT, email_type="Warning", missing_avro_files=None, dag_name=DAG_NAME)
        raise AirflowSkipException

    # move kv feeds from archival to ingress bucket
    move_file(ARCHIVE_BUCKET_NAME, INGRESS_BUCKET_NAME, source_files=kv_feed, destination_files=kv_feed)
    print("kv_feed_length:",len(kv_feed),"standard_feed_length:",len(standard_feed))

    # move standard feeds from archival to ingress bucket
    move_file(ARCHIVE_BUCKET_NAME, INGRESS_BUCKET_NAME, source_files=standard_feed, destination_files=standard_feed)
    return True

# supporting function for both standard and kv feeds to copy files from archival bucket
def fetch_feed_file_from_archive(common_prefix: str):
    # find directories with date eg : feeds/standard-feed/20220202/*
    blobs = storage_client_impersonated.list_blobs(ARCHIVE_BUCKET_NAME, prefix=common_prefix)
    file_path = [str(blob.name) for blob in blobs]    
    print(len(file_path))
    # move files back to ingress bucket
    move_file(ARCHIVE_BUCKET_NAME, INGRESS_BUCKET_NAME, source_files=file_path, destination_files=file_path)
    print(file_path)

    return True

# Copy auction kv feed avro files from archival bucket
def fetch_kv_feeds_from_archival(**kwargs):
    transaction_dt = get_transaction_dt()
    kv_feed_common_path = "feeds/auction_kv_labels_feed/{date}/".format(date=transaction_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)

# Copy standard feed avro files from archival bucket
def fetch_standard_feeds_from_archival(**kwargs):
    transaction_dt = get_transaction_dt()
    kv_feed_common_path = "feeds/standard_feed/{date}/".format(date=transaction_dt.strftime("%Y%m%d"))
    fetch_feed_file_from_archive(kv_feed_common_path)

# Check if the current transaction_dt in control table is history transaction_dt or not?
def detect_history_transaction_dt():
    transaction_dt = get_transaction_dt() 
    select_query = "SELECT transaction_dt, processed_flag, historical_file_flag \
FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` \
WHERE processed_flag='N' AND historical_file_flag='Y' and transaction_dt='{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt.strftime('%Y-%m-%d'))

    results = execute_query(bq_client, select_query)
    if results.total_rows == 0:
        return "skip_restore_archive_tasks"
    return 'fetch_manifest_files_from_archival'
  
#Step1: Reading yesterday date manifest files
def scan_bucket(**kwargs):
    prefix = "manifests/"
    blobs = storage_client_impersonated.list_blobs(INGRESS_BUCKET_NAME, prefix=prefix, delimiter="/")
    transaction_dt = get_transaction_dt()
    kv_feed_regex = prefix + KV_FEED_PREFIX + transaction_dt.strftime("%Y%m%d") + "\w+"
    standard_feed_regex = prefix + STD_FEED_PREFIX + transaction_dt.strftime("%Y%m%d") + "\w+"
    kv_feed = []
    standard_feed = []
    for blob in blobs:
        if re.match(kv_feed_regex, blob.name):
            kv_feed.append(blob.name)
        elif re.match(standard_feed_regex, blob.name):
            standard_feed.append(blob.name)
    if not (kv_feed and standard_feed):
        missing_transaction_dt_for_ctl_table_or_ingress_failure_email(dag_name=DAG_NAME, environment=ENVIRONMENT, email_type="Failure")
        raise Exception("No manifest files exist to process")
    return {"kv_feed": kv_feed, "standard_feed": standard_feed}

#Step2: Find deduplicate manifest files based on hour for the same day
#Step2.1: deduplicate manifest files for both kv and standard feeds
def remove_duplicate_hours(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='scan_bucket')

    filtered_kv_feeds = filter_duplicate_hours(feed_files['kv_feed'])            
    filtered_standard_feeds = filter_duplicate_hours(feed_files['standard_feed'])
    
    duplicate_file_list = filtered_kv_feeds["duplicate_file_list"] + filtered_standard_feeds["duplicate_file_list"]
    
    if duplicate_file_list:
        duplicate_hours_files_warning_email(environment=ENVIRONMENT, email_type="Warning", duplicate_hour_files=duplicate_file_list, dag_name=DAG_NAME)

    return {"kv_feed": filtered_kv_feeds, "standard_feed": filtered_standard_feeds}

#Step3.1: Archive duplicate manifest files
def archive_duplicate_files(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')

    destination_files_common_path = "manifests/duplicates/"

    duplicate_files_kv_feed = feed_files['kv_feed']['duplicate_file_list']
    duplicate_files_standard_feed = feed_files['standard_feed']['duplicate_file_list']

    destination_path_duplicate_kv_feeds = [destination_files_common_path + file.split("/")[-1] for file in
                                           duplicate_files_kv_feed]

    destination_path_duplicate_standard_feeds = [destination_files_common_path + file.split("/")[-1] for file in
                                                 duplicate_files_standard_feed]

    move_file(INGRESS_BUCKET_NAME, ARCHIVE_BUCKET_NAME,
              source_files=duplicate_files_kv_feed,
              destination_files=destination_path_duplicate_kv_feeds, delete=True)

    move_file(INGRESS_BUCKET_NAME, ARCHIVE_BUCKET_NAME,
              duplicate_files_standard_feed,
              destination_files=destination_path_duplicate_standard_feeds, delete=True)
    return True
   
#Step5: Supporting function to extract avro files path from manifest files
def extract_avro_files_path(bucket_name:str, blob_name:str):
    bucket = storage_client_impersonated.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_file_contents = json.loads(blob.download_as_string())
    path = json_file_contents['path'][1:]
    avro_files = [path + "/" + filepath['name'] for filepath in json_file_contents['files']]
    return avro_files

#Step6: Preparing avro files based on feed type (standard and kv)
def prepare_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')
    valid_kv_feeds_json = feed_files['kv_feed']['processed_files']
    valid_standard_feeds_json = feed_files['standard_feed']['processed_files']

    kv_feed_avro_file = []
    standard_feed_avro_file = []
    for file in valid_kv_feeds_json:
        kv_feed_avro_file.extend(extract_avro_files_path(bucket_name=INGRESS_BUCKET_NAME, blob_name=file))

    for file in valid_standard_feeds_json:
        standard_feed_avro_file.extend(extract_avro_files_path(bucket_name=INGRESS_BUCKET_NAME, blob_name=file))

    print(kv_feed_avro_file, standard_feed_avro_file)
    xComm_var.xcom_push(key='kv_feed_avro_file', value=kv_feed_avro_file)
    xComm_var.xcom_push(key="standard_feed_avro_file", value=standard_feed_avro_file)
    return True
    
#Step7: Supporting function for validating file exists    
def is_path_exists(bucket_name: str, files_list: list):
    missing_files = []
    valid_avro_files = files_list.copy()
    for path in files_list:
        bucket = storage_client_impersonated.bucket(bucket_name)
        blob = bucket.blob(path)
        if not blob.exists():
            missing_files.append(path)
            valid_avro_files.remove(path)
    
    return valid_avro_files, missing_files

#Step7.1: Validate avro files path
def check_valid_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    kv_feed_avro_files = xComm_var.xcom_pull(key='kv_feed_avro_file', task_ids='prepare_avro_files')
    standard_feed_avro_files = xComm_var.xcom_pull(key='standard_feed_avro_file', task_ids='prepare_avro_files')
    
    kv_feed_valid_avro_files, kv_feed_missing_avro_files  = is_path_exists(bucket_name=INGRESS_BUCKET_NAME, files_list=kv_feed_avro_files)
    standard_feed_valid_avro_files, standard_feed_missing_avro_files = is_path_exists(bucket_name=INGRESS_BUCKET_NAME,files_list=standard_feed_avro_files)
    
    if not (kv_feed_valid_avro_files and standard_feed_valid_avro_files):
        missing_avro_feed_failure_email(dag_name=DAG_NAME, environment=ENVIRONMENT, email_type="Failure")
        raise Exception("No standard and auction kv feed avro files exist to process")
    missing_avro_files = kv_feed_missing_avro_files + standard_feed_missing_avro_files
    if missing_avro_files:
        missing_avro_files_warning_email(environment=ENVIRONMENT, email_type="Warning", missing_avro_files=missing_avro_files, dag_name=DAG_NAME)
        
    xComm_var.xcom_push(key='kv_feed_avro_file', value=kv_feed_valid_avro_files)
    xComm_var.xcom_push(key="standard_feed_avro_file", value=standard_feed_valid_avro_files)

#Step8: Supporting function to load standard feed based on defined schema
def load_std_feed_avro_file(avro_files_path):  # Load AppNexus, or APN, standard avro file from Cloud Storage.
    # Construct a BigQuery client object.
    client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

    # Set table_id to the ID of the table to create.
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.std_feed"

    job_config = bigquery.LoadJobConfig(
       schema=[bigquery.SchemaField("auction_id_64", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("is_imp", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("publisher_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("site_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("tag_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("insertion_order_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("line_item_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("creative_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("is_click", "INTEGER", mode="REQUIRED"),
        ],
       write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
       source_format=bigquery.SourceFormat.AVRO,
     )
    
    gs_path = [f"gs://{INGRESS_BUCKET_NAME}/" + path for path in avro_files_path]
    uri = gs_path

    load_job = client.load_table_from_uri( uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows to table {}.{}".format(destination_table.num_rows,destination_table.dataset_id, destination_table.table_id))

#Step9: Supporting function to load kv feed based on defined schema
def load_kv_feed_avro_file(avro_files_path):  # Load AppNexus, or APN, key value avro file from Cloud Storage.
    
    # Construct a BigQuery client object.
    client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

    # Set table_id to the ID of the table to create.
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.kv_feed"

    job_config = bigquery.LoadJobConfig(
       schema=[bigquery.SchemaField("date_time", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("auction_id_64", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
        ],
       write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
       source_format=bigquery.SourceFormat.AVRO,
     )

    gs_path = [f"gs://{INGRESS_BUCKET_NAME}/" + path for path in avro_files_path]
    uri = gs_path

    load_job = client.load_table_from_uri( uri, table_id, job_config=job_config)  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows to table {}.{}".format(destination_table.num_rows,destination_table.dataset_id, destination_table.table_id))

#Step8.1: Load standard avro file data into BigQuery std_feed table
def load_std_feed_avro_files_to_bq(**kwargs):
    xComm_var = kwargs['ti']
    standard_feed_avro_files = xComm_var.xcom_pull(key='standard_feed_avro_file', task_ids='check_valid_avro_files')
    print(standard_feed_avro_files)
    load_std_feed_avro_file(standard_feed_avro_files)

# --------------------------------------------------------------------------------
# Step8.2: Email: Ensure all std.tag_id values are in placements.placement_id
# NEW CHECK to be added due to Transaction Prod job failure FEB 27
# DEFINE EMAIL FOR TAG ID IN STANDARD FEED IS NOT MATCHING TO PLACEMENT ID IN PLACEMENTS
# --------------------------------------------------------------------------------
def run_query_placement_tag(**kwargs):
    query_job = bq_client.query(
        """SELECT tag_id
            FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.std_feed`
            EXCEPT DISTINCT
            SELECT  placement_id as tag_id 
            FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.placements`;
           """.format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    )
    
    placement_tag_id_results = []    
    results = query_job.result()
        
    for row in results:
        placement_tag_id_results.append("{}".format(row.tag_id))
    if placement_tag_id_results:
        missing_placement_warning_email(environment=ENVIRONMENT, email_type="Warning", result_rows=placement_tag_id_results, dag_name=DAG_NAME)

# missing_placement_warning_email
def missing_placement_warning_email(environment, email_type, result_rows, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Missing Placement Warning</strong>: The incoming AppNexus standard feed data contains a tag_id value which is missing from our hard-coded placements table in the placement_id column. <br/></br>"""
    email_body += """<strong>Standard feed tag_id values: </strong>"""
    email_body += """<ul>"""
    for filename in result_rows:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""
    
    email_body += """&emsp;&emsp;SELECT tag_id FROM %s.%s.std_feed <br> &emsp;&emsp;EXCEPT DISTINCT <br> &emsp;&emsp;SELECT placement_id as tag_id FROM %s.%s.placements<br/></br>""" %(PIPELINE_PROJECT_ID, PIPELINE_DATASET_ID, PIPELINE_PROJECT_ID, PIPELINE_DATASET_ID)
    email_body += """<strong>Actions</strong>:</br>"""
    email_body += """<ul>"""
    email_body += """<li>Create SNOW ticket to add missing row(s) to %s placements table and update the NULL values in the %s adserver_transaction table.  Also, for each standard feed tag_id above, add missing row(s) of placement information to placements DAG SQL; run in DEV; promote changes to CERT & PROD.</br>""" %(environment, environment)
    email_body += """</ul>"""
          
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

#Step9.1: Load kv avro file data into BigQuery kv_feed table
def load_kv_feed_avro_files_to_bq(**kwargs):
    xComm_var = kwargs['ti']
    kv_feed_avro_files = xComm_var.xcom_pull(key='kv_feed_avro_file', task_ids='check_valid_avro_files')
    print(kv_feed_avro_files)
    load_kv_feed_avro_file(kv_feed_avro_files)

#Step10: Archive processed manifest files
def archive_valid_manifest_files(**kwargs):
    xComm_var = kwargs['ti']
    feed_files = xComm_var.xcom_pull(task_ids='remove_duplicate_hours')
    valid_files_kv_feed = feed_files['kv_feed']['processed_files']
    valid_files_standard_feed = feed_files['standard_feed']['processed_files']

    move_file(INGRESS_BUCKET_NAME, ARCHIVE_BUCKET_NAME,
              source_files=valid_files_kv_feed,
              destination_files=valid_files_kv_feed, delete=True)

    move_file(INGRESS_BUCKET_NAME, ARCHIVE_BUCKET_NAME,
              source_files=valid_files_standard_feed,
              destination_files=valid_files_standard_feed, delete=True)

    return True

#Step11: Archive avro files after loading data
def archive_valid_avro_files(**kwargs):
    xComm_var = kwargs['ti']
    transaction_dt = xComm_var.xcom_pull(task_ids='update_processed_flag_for_current_transaction')
    archive_avro_files(avro_dt_directory_path='feeds/auction_kv_labels_feed/' + transaction_dt)
    archive_avro_files(avro_dt_directory_path='feeds/standard_feed/' + transaction_dt)
    
# General function for archive avro files
def archive_avro_files(avro_dt_directory_path):
    blobs = storage_client_impersonated.list_blobs(INGRESS_BUCKET_NAME, prefix=avro_dt_directory_path)
    blobs_list = [blob.name for blob in blobs]
    move_file(bucket_name=INGRESS_BUCKET_NAME, destination_bucket_name=ARCHIVE_BUCKET_NAME, source_files=blobs_list,
              destination_files=blobs_list, delete=True)
    return None
              
# --------------------------------------------------------------------------------
# Delete transaction_audit_log_stg
# --------------------------------------------------------------------------------
'''
DELETE from `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` where 1=1;
'''

delete_stg_audit_table = BigQueryExecuteQueryOperator(   
    task_id='delete_stg_audit_table',
    sql='delete_stage_audit_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)
        
#**************************************************************#
# Get APN impression count
#**************************************************************#
#Step0: Secret value credentials function to authenticate API
def access_secret_value(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    """

    # Import the Secret Manager client library.
    from google.cloud import secretmanager
        
    client = get_sm_impersonated_client(SERVICE_ACCOUNT,GOVERNANCE_PROJECT_ID)
 
    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return secret payload.
    payload = response.payload.data.decode("UTF-8")
    return payload

#Step1: API authentication
def api_auth():
    username=access_secret_value(GOVERNANCE_PROJECT_ID,'promospots-ads_apn_username', 'latest')
    password=access_secret_value(GOVERNANCE_PROJECT_ID,'promospots-ads_apn_pass', 'latest')
    data = json.dumps({"auth": {"username": username, "password": password}})
    API_ENDPOINT = "https://api.appnexus.com/auth"
    print("DATA:",data)
    # sending post request and saving response as response object
    r = requests.post(url=API_ENDPOINT, data=data)
    # extracting response text
    pastebin_url = r.text
    print("The pastebin URL is:%s" % pastebin_url)
    resp = pastebin_url
    return pastebin_url

#Step2: POST request to APN API
def api_post(**kwargs):
    xComm_var = kwargs['ti']
    api_response = xComm_var.xcom_pull(task_ids='api_authentication')
    api_response = json.loads(api_response)
    url = "https://api.appnexus.com/report"
    apn_start_date = get_transaction_dt()
    apn_end_date = get_transaction_dt() + timedelta(1)
    apn_fmtd_start_date = apn_start_date.strftime('%Y-%m-%d')
    apn_fmtd_end_date = apn_end_date.strftime('%Y-%m-%d')
    print(apn_fmtd_start_date,apn_fmtd_end_date)
    try:    
        data = {
        "report":
        {
            "report_type":"network_analytics",
            "columns":[
                "day",
                "imps",
                "clicks"
            ],
            "start_date":apn_fmtd_start_date,
            "end_date":apn_fmtd_end_date,
            "format":"csv"
        }
        }
        token = api_response['response']['token']
        headers = {'Authorization': "Bearer {}".format(token)}
        r = requests.post(url,  headers=headers, data=json.dumps(data))
        print(r.text)
        r = json.loads(r.text)
        report_id = r['response']['report_id']
        return {'token': token, 'report_id': report_id}
    except Exception as e:
        print(str(e))
        return {'token': None, 'report_id': None}

#Step3: Download the impression count file
def download_file(**kwargs):
    xComm_var = kwargs['ti']
    response = xComm_var.xcom_pull(task_ids='api_post')
    report_id = response['report_id']
    token = response['token']
    if report_id is not None:
        url= "https://api.appnexus.com/report-download?id=" + report_id
        print(url)
        headers = {'Authorization': "Bearer {}".format(token)}
        r = requests.get(url, headers=headers)
        data = r.text
        if len(data.strip()) > 2:
            print(data)
            return data
    transaction_dt = get_transaction_dt()
    dummy_data = "day,imps,clicks\n{transaction_dt},0,0".format(transaction_dt=transaction_dt.strftime('%Y-%m-%d'))
    api_warning_email(environment=ENVIRONMENT, email_type="Warning", missing_avro_files=None, dag_name=DAG_NAME)
    return dummy_data

#Step4: Load impression count file data into BQ    
def load_bq(**kwargs):
    x_Comm_var = kwargs['ti']
    data = x_Comm_var.xcom_pull(task_ids='download_file')
    csv_data = list(filter(bool, data.splitlines()))
    columns = csv_data.pop(0).split(",")

    dataset_name = LAKEHOUSE_DATASET_ID
    table_name = "adserver_transaction"
    
    csv_table = {}
    for row_no in range(len(columns)):
        csv_table[columns[row_no]] =[val.split(",")[row_no] for val in csv_data]

    values_fmt = "('{dataset_name}', '{table_name}', '{transaction_dt}', {apn_impression_count}, '{load_ts}'),"
    values_sql = ""
    for row_no in range(len(csv_data)):
        values_sql += values_fmt.format(dataset_name=dataset_name, table_name=table_name,
                                        transaction_dt=csv_table['day'][row_no],
                                        apn_impression_count=csv_table['imps'][row_no],
                                        load_ts=load_ts)
    values_sql = values_sql[:-1]


    sql_Statement = "INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.transaction_audit_log_stg` " \
                    "(dataset_name, table_name, transaction_dt, apn_impression_count, load_ts) " \
                    "VALUES " + values_sql
    sql = sql_Statement.format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    final_sql=sql
    print(final_sql)
    kwargs['ti'].xcom_push(key='sql_key', value=final_sql)
    return final_sql

# --------------------------------------------------------------------------------
# DEFINE EMAIL FOR OTH ACTION CODE GIVEN TO AN UNMAPPED PAGE CODE
# oth_warning_email query check
# --------------------------------------------------------------------------------
def run_query_oth(**kwargs):
    query_job = bq_client.query(
        """SELECT page_cd
           FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_adserver_trans`
           WHERE action_cd ='OTH' 
           EXCEPT DISTINCT
           SELECT page_cd 
           FROM `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.adserver_transaction`
           WHERE action_cd='OTH'""".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,LAKEHOUSE_PROJECT_ID=LAKEHOUSE_PROJECT_ID,LAKEHOUSE_DATASET_ID=LAKEHOUSE_DATASET_ID)
    )
    
    oth_results = []    
    results = query_job.result()
        
    for row in results:
        oth_results.append("{}".format(row.page_cd))
    if oth_results:
        oth_warning_email(environment=ENVIRONMENT, email_type="Warning", result_rows=oth_results, dag_name=DAG_NAME)


# oth_warning_email 
def oth_warning_email(environment, email_type, result_rows, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: One or more unmapped page codes were added to promospots_ads.adserver_transaction table in today's processing. <br/></br>"""
    email_body += """<strong>Corresponding Page code(s): </strong></br></br>"""
    email_body += """<ul>"""
    for filename in result_rows:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""
    
    email_body += """<strong>Action</strong>: Review with business team to check if new page code(s) should be added to promospots_ads_stg.page_cd_to_action_cd_map table."""

    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# --------------------------------------------------------------------------------
# DEFINE EMAIL FOR CITY NAMES RECEIVED INSTEAD OF AN AIRPORT CODE 
# --------------------------------------------------------------------------------
     
# city_warning_email query check
def run_query_city(**kwargs):
    query_job = bq_client.query(
        """SELECT distinct kv_ecy_start_city_value, kv_aac_end_city_value 
           FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_adserver_trans_init`
           WHERE ((length(kv_ecy_start_city_value) > 3) or (length(kv_aac_end_city_value) > 3 ));""".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    )

    result_rows = []
    results = query_job.result()
    
    for row in results:
        result_rows.append("{} | {}".format(row.kv_ecy_start_city_value, row.kv_aac_end_city_value))
    if result_rows:
        city_warning_email(environment=ENVIRONMENT, email_type="Warning", result_rows=result_rows, dag_name=DAG_NAME)


# city_warning_email 
def city_warning_email(environment, email_type, result_rows, dag_name, product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: The following Segment Start or End City Code value(s) with more than three characters was/were received today. <br/></br>"""
    email_body += """<strong>Start_city_value | end_city_value (** The value of 'None' indicates the field is blank.): </strong></br></br>"""
    email_body += """<ul>"""
    for filename in result_rows:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""

    email_body += """<strong>Action</strong>: Request database change to update corresponding Segment Start or End City Code value(s) to reflect correct three-character city code(s).<br/></br>"""
    email_body += """&emsp;&emsp;SELECT distinct kv_ecy_start_city_value, kv_aac_end_city_value FROM `%s.%s.adserver_transaction` <br> &emsp;&emsp;WHERE ((length(kv_ecy_start_city_value) > 3) or (length(kv_aac_end_city_value) > 3 )); <br/></br>""" %(LAKEHOUSE_PROJECT_ID, LAKEHOUSE_DATASET_ID)
    email_body += """&emsp;&emsp;UPDATE `%s.%s.adserver_transaction` SET segment_start_city_cd = '?' WHERE segment_start_city_cd = '?' AND transaction_dt = '?'; <br/></br>""" %(LAKEHOUSE_PROJECT_ID, LAKEHOUSE_DATASET_ID)
    email_body += """&emsp;&emsp;UPDATE `%s.%s.adserver_transaction` SET segment_end_city_cd = '?' WHERE segment_end_city_cd = '?' AND transaction_dt = '?';""" %(LAKEHOUSE_PROJECT_ID, LAKEHOUSE_DATASET_ID)

    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# --------------------------------------------------------------------------------
# SQL 1 - Processing APN avro files loaded into "std_feed" and "kv_feed" stage tables, insert results into wads_adserver_trans_init
# Note: placements table (promospots_ads_stg.placements) in Transformation STEP 1 is a reference table created from APN data. 
# --------------------------------------------------------------------------------

'''
insert into sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init (transaction_dt, campaign_id, creative_id, site_cd, page_cd, 
    position_cd, host_cd, pcc, segment_start_dt, kv_ecy_start_city_value, kv_aac_end_city_value, impressions, clicks, load_dt)
SELECT
    std.transaction_dt, std.campaign_id, std.creative_id, pl.site_cd, pl.page_cd,
    pl.position_cd, kv.host_cd, kv.pcc, kv.segment_start_dt, kv.kv_ecy_start_city_value, kv.kv_aac_end_city_value,
    sum(std.impressions) as impressions, sum(std.clicks) as clicks, CURRENT_DATE as load_dt
    FROM
    (
        SELECT 
    auction_id_64, tag_id,
    CAST(TIMESTAMP_SECONDS(std_feed.date_time) as DATE) as transaction_dt,
    std_feed.site_id,
    std_feed.line_item_id as campaign_id,
    std_feed.creative_id,
    sum(std_feed.is_imp) as impressions,
    sum(std_feed.is_click) as clicks
    FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.std_feed` std_feed
    group by auction_id_64,
    tag_id,
    transaction_dt, 
    std_feed.publisher_id,
    std_feed.site_id, 
    std_feed.insertion_order_id,
    std_feed.line_item_id, 
    std_feed.creative_id
    ) AS std
    ON std.auction_id_64 = kv.auction_id_64
    JOIN
        (
        SELECT 
    placements.placement_id, 
    placements.publisher_name as site_cd,
    placements.page_cd, 
    placements.position as position_cd
    FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.placements` placements
    ) AS pl
    on std.tag_id = pl.placement_id
    LEFT JOIN
        (
        SELECT kv_feed.auction_id_64,
    CAST(TIMESTAMP_SECONDS(kv_feed.date_time) as DATE) as transaction_dt_kv,
    max(case when kv_feed.key='hst' then value end) host_cd,
    max(case when kv_feed.key='pcc' then value end) pcc,
    PARSE_DATE("%Y%m%d",(max(case when kv_feed.key='pdt' then value end))) segment_start_dt,
    max(case when kv_feed.key='ecy' then value end) kv_ecy_start_city_value,
    max(case when kv_feed.key='aac' then value end) kv_aac_end_city_value
    FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.kv_feed` kv_feed
    group by 
    kv_feed.auction_id_64,
    transaction_dt_kv
    ) AS kv    
    group by 
    std.transaction_dt,
    std.campaign_id,
    std.creative_id,
    pl.site_cd, 
    pl.page_cd,
    pl.position_cd,
    kv.host_cd,
    kv.pcc,
    kv.segment_start_dt,
    kv.kv_ecy_start_city_value,
    kv.kv_aac_end_city_value
    ;
'''

load_into_wads_adserver_trans_init = BigQueryExecuteQueryOperator(
    task_id='load_into_wads_adserver_trans_init',
    sql='01_apn_data_load_into_wads_adserver_trans_init.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 2
# Update transaction_audit_log_stg with impression and row count from wads_adserver_trans_init
# --------------------------------------------------------------------------------

'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS
SET
  TALS.stage_rows_loaded_count = WATI.stage_rows_loaded_count,
  TALS.stage_impression_count = WATI.stage_impression_count
FROM (
  SELECT
    transaction_dt,
    COUNT(*) AS stage_rows_loaded_count,
    SUM(impressions) AS stage_impression_count
  FROM
    `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init`
  GROUP BY
    transaction_dt ) WATI
WHERE
  TALS.transaction_dt = WATI.transaction_dt;
'''

wads_adserver_trans_init_row_and_impression_counts_added_to_audit_stg = BigQueryExecuteQueryOperator(
    task_id='wads_adserver_trans_init_row_and_impression_counts_added_to_audit_stg',
    sql='02_wads_adserver_trans_init_row_and_impression_counts_added_to_audit_stg.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 3
# Verify data transformation contains no duplicates. This functionality was replicated from step 18 in ODI code to "check_dupes"
# --------------------------------------------------------------------------------

'''
----****Variable:  ADSL.CheckDupes 
/*
SQL:  SELECT COUNT(*) FROM ( 
SELECT CampaignId 
FROM Stage_DB.SADS_AdServerTrans_Init 
HAVING COUNT(*)>1 
GROUP BY 
TransactionDate,
CampaignId,
CreativeId, 
SiteCd , 
PageCd , 
PositionCd , 
HostCd , 
ActionCd , 
PCC , 
SegmentStartDate , 
SegmentStartCityCd , 
SegmentEndCityCd) Inner_Query 
*/

/*
SELECT COUNT(*) FROM (
    SELECT campaign_id
    FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init`
    GROUP BY
    transaction_dt,
    campaign_id,
    creative_id,
    site_cd,
    page_cd,
    position_cd,
    host_cd,
    action_cd,
    pcc,
    segment_start_dt,
    kv_ecy_start_city_value,
    kv_aac_end_city_value
    HAVING COUNT(*)>1
    ) Inner_Query;
*/
# TASK ADDED IN DATA DISRUPTION STORY, to include transaction_dt in where clause
    
/*
UPDATE `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS
SET TALS.rows_duplicated_count = WATI.rows_duplicated_count
FROM
(
SELECT COUNT(*) AS rows_duplicated_count FROM (
    SELECT campaign_id
    FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init`
    GROUP BY
    transaction_dt,
    campaign_id,
    creative_id,
    site_cd,
    page_cd,
    position_cd,
    host_cd,
    action_cd,
    pcc,
    segment_start_dt,
    kv_ecy_start_city_value,
    kv_aac_end_city_value
    HAVING COUNT(*)>1
    ) Inner_Query
) WATI
WHERE TALS.rows_duplicated_count is null

*/
'''
check_dupes_wads_adserver_trans_init = BigQueryExecuteQueryOperator(
    task_id='check_dupes_wads_adserver_trans_init',
    sql='03_check_dupes_wads_adserver_trans_init.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 4
# update "transaction_audit_log_stg.rows_duplicated_count" when previous step has not entered a zero count 
# This step replicates ODI code  if equal to 0 then ODI skipped this step
# This step replicates ODI step 19 and will update transaction_audit_log_stg "rows_duplicated_count" only when previous step does not update "rows_duplicated_count" with 0.  
# --------------------------------------------------------------------------------
'''
---****If ADSL.CheckDupes > 0, then  
/*
Variable:  ADSL.CountRecordsDupes 
SQL:  
    SELECT SUM(c1) FROM ( 
    SELECT COUNT(*) AS c1 FROM Stage_DB.SADS_AdServerTrans_Init 
    HAVING COUNT(*)>1 
    GROUP BY 
    TransactionDate, 
    CampaignId, 
    CreativeId, 
    SiteCd , 
    PageCd , 
    PositionCd , 
    HostCd , 
    ActionCd , 
    PCC , 
    SegmentStartDate , 
    SegmentStartCityCd , 
    SegmentEndCityCd ) a 
    ;   
*/
UPDATE `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS

SET TALS.rows_duplicated_count = WATI.rows_duplicated_count
FROM
(
SELECT transaction_dt,SUM(c1) AS rows_duplicated_count FROM (
SELECT transaction_dt, COUNT(*) as c1 FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init`
    GROUP BY
    transaction_dt,
    campaign_id,
    creative_id,
    site_cd,
    page_cd,
    position_cd,
    host_cd,
    action_cd,
    pcc,
    segment_start_dt,
    kv_ecy_start_city_value,
    kv_aac_end_city_value
    HAVING COUNT(*)>1
    ) a
    GROUP BY transaction_dt
	) WATI
 where WATI.transaction_dt = TALS.transaction_dt
 and TALS.rows_duplicated_count <> 0;

'''
count_duplicate_rows_in_wads_adserver_trans_init = BigQueryExecuteQueryOperator(
    task_id='count_duplicate_rows_in_wads_adserver_trans_init',
    sql='04_count_rows_dupes_in_wads_adserver_trans_init.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
) 

# --------------------------------------------------------------------------------
# TRANSFORMATION
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
# SQL 5
# Insert wads_adserver_trans_init  into wads_adserver_trans for data transformation
# Transform page_cd, host_cd, pcc, segment_start_city_cd, segment_end_city_cd data to UPPER case. 
# --------------------------------------------------------------------------------
'''
INSERT INTO
    `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans` (transaction_dt,
    campaign_id,
    creative_id,
    site_cd,
    page_cd,
    position_cd,
    host_cd,
    pcc,
    segment_start_dt,
    kv_ecy_start_city_value,
    segment_start_city_cd,
    kv_aac_end_city_value,
    segment_end_city_cd,
    impressions,
    clicks,
    load_dt)
SELECT
  transaction_dt,
  campaign_id,
  creative_id,
  site_cd,
  UPPER(page_cd),
  position_cd,
  UPPER(host_cd),
  UPPER(pcc),
  segment_start_dt,
  kv_ecy_start_city_value,
  UPPER(kv_ecy_start_city_value) AS segment_start_city_cd,
  kv_aac_end_city_value,
  UPPER(kv_aac_end_city_value) AS segment_end_city_cd,
  impressions,
  clicks,
  load_dt
FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans_init`
  ;
'''
insert_into_wads_adserver_trans = BigQueryExecuteQueryOperator(   
    task_id='insert_into_wads_adserver_trans',
    sql='05_insert_wads_adserver_trans_init_into_wads_adserver_trans.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
) 

# --------------------------------------------------------------------------------
# SQL 6
# Update null action_cd with action_cd from promospots_ads_stg.page_cd_to_action_cd_map for page_cd received. 
# Currently the reporting process is using action_cd. More work to be consiered in reporting process to use page codes insted of action_cd, to remove the dependecy of "action_cd". 
# --------------------------------------------------------------------------------
'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans` WAT
SET
  WAT.action_cd = PCAM.action_cd
FROM
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.page_cd_to_action_cd_map` PCAM
WHERE WAT.page_cd = PCAM.page_cd
  AND WAT.action_cd IS NULL
  AND WAT.page_cd IS NOT NULL
  ;
'''
update_action_cd_mapped = BigQueryExecuteQueryOperator(   
    task_id='update_action_cd_mapped',
    sql='06_update_wads_adserver_trans_with_action_cd_mapped.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

# --------------------------------------------------------------------------------
# Email: Check for City Names received for expected City Code
# --------------------------------------------------------------------------------

run_query_city = PythonOperator(
    task_id='run_query_city',
    python_callable=run_query_city,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 7
# Update action_cd with OTH for any page_cd not listed in promospots_ads_stg.page_cd_to_action_cd_map
# --------------------------------------------------------------------------------
'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans` WAT
SET
  WAT.action_cd = 'OTH'
WHERE WAT.action_cd IS NULL
  AND WAT.page_cd IS NOT NULL
  ;
'''
update_OTH_action_cd = BigQueryExecuteQueryOperator(   
    task_id='update_OTH_action_cd',
    sql='07_update_wads_adserver_trans_oth_action_cd.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
) 

# --------------------------------------------------------------------------------
# TARGET
# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------
# SQL 8
# Check target table for any existing rows currently entered for the staged transaction date impressions.
# If target table contains rows for the staged tranaction date move rows to adserver_trans_old_rows_removed.
# --------------------------------------------------------------------------------
'''
INSERT INTO `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_trans_old_rows_removed` 
(transaction_dt,
campaign_id,
creative_id,
site_cd,
page_cd,
position_cd,
host_cd,
action_cd,
pcc,
segment_start_dt,
kv_ecy_start_city_value,
segment_start_city_cd,
kv_aac_end_city_value,
segment_end_city_cd,
impressions,
clicks,
original_load_dt,
load_ts
)
SELECT 
transaction_dt,
campaign_id,
creative_id,
site_cd,
page_cd,
position_cd,
host_cd,
action_cd,
pcc,
segment_start_dt,
kv_ecy_start_city_value,
segment_start_city_cd,
kv_aac_end_city_value,
segment_end_city_cd,
impressions,
clicks,
load_dt AS original_load_dt,
CURRENT_TIMESTAMP AS load_ts
FROM  `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_transaction` ATT
WHERE ATT.transaction_dt IN 
(SELECT DISTINCT transaction_dt FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans`)
;
'''
insert_existing_transaction_dt_into_adserver_transaction_old_rows_removed = BigQueryExecuteQueryOperator(   
    task_id='insert_existing_transaction_dt_into_adserver_transaction_old_rows_removed',
    sql='08_insert_into_adserver_transaction_old_rows_removed.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)  

# --------------------------------------------------------------------------------
# SQL 9
# Update "transaction_audit_log_stg.rows_removed_count" with the count of removed rows.
# --------------------------------------------------------------------------------
'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS
SET
  TALS.rows_removed_count = ATRR.rows_removed_count
FROM (
  SELECT
    orrc.transaction_dt,
    orrc.load_ts,
    orrc.rows_removed_count
  FROM ( (
      SELECT
        transaction_dt,
        load_ts,
        COUNT(*) AS rows_removed_count
      FROM
        `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_trans_old_rows_removed`
      GROUP BY
        1,2
         ) ORRC
    JOIN (
      SELECT
        transaction_dt,
        MAX(load_ts) AS load_ts
      FROM
        `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_trans_old_rows_removed`
      GROUP BY
        1 ) ORR
    ON
      ORR.transaction_dt = ORRC.transaction_dt
      AND ORRC.load_ts = ORR.load_ts )
     ) ATRR
WHERE
  ATRR.transaction_dt = TALS.transaction_dt;

'''
update_audit_rows_removed_from_adserver_transaction_old_rows_removed = BigQueryExecuteQueryOperator(   
    task_id='update_audit_rows_removed_from_adserver_transaction_old_rows_removed',
    sql='09_update_audit_rows_removed_from_adserver_transaction_old_rows_removed.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)  

# --------------------------------------------------------------------------------
# EMAIL: If a new OTH is added in wads_adserver_trans, then send email to notify that a page_cd was received which is not mapped in promospots_ads_stg.page_cd_to_action_cd_map
# Send a warning email when new unmapped page_cd is received to notify that a new PageCd has been loaded into adserver.transaction.
# This email will list new page_cd received. Business team will review the new page_cd and notify dev team when a new page_cd map should be added to promospots_ads_stg.page_cd_to_action_cd_map`. 
# --------------------------------------------------------------------------------
'''  
SELECT
  page_cd
FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans` 
WHERE  action_cd='OTH'

   EXCEPT DISTINCT 

SELECT 
 page_cd
FROM `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_transaction` 
 where action_cd = 'OTH' 

'''	
run_query_oth = PythonOperator(
    task_id='run_query_oth',
    python_callable=run_query_oth,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 10
# TARGET - delete from "adserver_transaction" the rows matching "transaction_dt" in "wads_adserver_trans"
# --------------------------------------------------------------------------------
'''
DELETE
FROM
  `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_transaction`
WHERE
  transaction_dt IN (
  SELECT
    DISTINCT transaction_dt
  FROM
    `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans`);
'''
delete_adserver_transaction_transaction_dt_in_wads_adserver_trans = BigQueryExecuteQueryOperator(   
    task_id='delete_adserver_transaction_transaction_dt_in_wads_adserver_trans',
    sql='10_delete_adserver_transaction_transaction_dt_in_wads_adserver_trans.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
) 

# --------------------------------------------------------------------------------
# SQL 11
# TARGET - insert into "adserver_transaction" transaction date processed in "wads_adserver_trans"
# --------------------------------------------------------------------------------
'''
INSERT INTO
    `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_transaction` (transaction_dt,
    campaign_id,
    creative_id,
    site_cd,
    page_cd,
    position_cd,
    host_cd,
    action_cd,
    pcc,
    segment_start_dt,
    kv_ecy_start_city_value,
    segment_start_city_cd,
    kv_aac_end_city_value,
    segment_end_city_cd,
    impressions,
    clicks,
    load_dt)
SELECT
  transaction_dt,
  campaign_id,
  creative_id,
  site_cd,
  page_cd,
  position_cd,
  host_cd,
  action_cd,
  pcc,
  segment_start_dt,
  kv_ecy_start_city_value,
  segment_start_city_cd,
  kv_aac_end_city_value,
  segment_end_city_cd,
  impressions,
  clicks,
  load_dt
FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.wads_adserver_trans`
  ;
'''
insert_adserver_transaction = BigQueryExecuteQueryOperator(   
    task_id='insert_adserver_transaction',
    sql='11_insert_into_adserver_transaction.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)  

# --------------------------------------------------------------------------------
# SQL 12
# AUDIT updates from target
# Update "transaction_audit_log_stg.target_impression_count" and "target_rows_loaded_count". 
# --------------------------------------------------------------------------------
'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS
SET
  TALS.target_impression_count = ADST.target_impression_count,
  TALS.target_rows_loaded_count = ADST.target_rows_loaded_count
FROM (
  SELECT
    transaction_dt,
    COUNT(*) AS target_rows_loaded_count,
    SUM(impressions) AS target_impression_count
  FROM
    `sab-dev-dap-lakehouse-1470.promospots_ads.adserver_transaction`
  GROUP BY
    transaction_dt ) ADST
WHERE
  TALS.transaction_dt = ADST.transaction_dt;
'''
target_row_and_impression_counts_added_to_audit_stg = BigQueryExecuteQueryOperator(   
    task_id='target_row_and_impression_counts_added_to_audit_stg',
    sql='12_target_row_and_impression_counts_added_to_audit_stg.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
) 

# --------------------------------------------------------------------------------
# SQL 13
# Update "transaction_audit_log_stg.row_count_diff_stg_to_tgt" (stage_rows_loaded_count minus target_rows_loaded_count).  
# --------------------------------------------------------------------------------
'''
UPDATE
  `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` TALS
SET
  TALS.row_count_diff_stg_to_tgt = TALS2.row_count_diff_stg_to_tgt
FROM (
  SELECT
    transaction_dt,
    (stage_rows_loaded_count-target_rows_loaded_count) AS row_count_diff_stg_to_tgt
  FROM
    `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg`) TALS2
WHERE
  TALS.transaction_dt = TALS2.transaction_dt;
'''
row_count_diff_stg_to_tgt_added_to_audit_stg = BigQueryExecuteQueryOperator(   
    task_id='row_count_diff_stg_to_tgt_added_to_audit_stg',
    sql='13_row_count_diff_stg_to_tgt_added_to_audit_stg.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

# --------------------------------------------------------------------------------
# SQL 14
# insert "transaction_audit_log_stg" into target table "transaction_audit_log"
# --------------------------------------------------------------------------------
'''
INSERT INTO `sab-dev-dap-lakehouse-1470.promospots_ads.transaction_audit_log` 
(dataset_name,
table_name,
transaction_dt,
apn_impression_count,
stage_impression_count,
target_impression_count,
stage_rows_loaded_count,
target_rows_loaded_count,
rows_removed_count,
rows_duplicated_count,
row_count_diff_stg_to_tgt,
load_ts)
SELECT 
dataset_name,
table_name,
transaction_dt,
apn_impression_count,
stage_impression_count,
target_impression_count,
stage_rows_loaded_count,
target_rows_loaded_count,
rows_removed_count,
rows_duplicated_count,
row_count_diff_stg_to_tgt,
load_ts
FROM `sab-dev-dap-data-pipeline-3013.promospots_ads_stg.transaction_audit_log_stg` 
;
'''
insert_into_target_transaction_audit_log = BigQueryExecuteQueryOperator(   
    task_id='insert_into_target_transaction_audit_log',
    sql='14_insert_into_target_transaction_audit_log.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)  

# --------------------------------------------------------------------------------
# Email: create success/warning email for auditing impression counts
# --------------------------------------------------------------------------------

def execute_report_query(query):
    query_job = bq_client.query(query)
    results = query_job.result()
    result_rows = []
    for row in results:
        result_rows.extend([row.apn_impression_count, row.target_impression_count])
    if not result_rows:
        result_rows.extend([0,0])
    return result_rows

def transaction_report_email(**kwargs):
    transaction_dt = get_transaction_dt()
    print(transaction_dt)
    query_1 ="SELECT distinct tal.apn_impression_count,tal.target_impression_count \
FROM `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` tal \
where tal.transaction_dt='{transaction_dt}' and load_ts in (SELECT max(load_ts) \
from `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` ls \
where ls.transaction_dt=tal.transaction_dt)".format(transaction_dt=transaction_dt,LAKEHOUSE_PROJECT_ID=LAKEHOUSE_PROJECT_ID,LAKEHOUSE_DATASET_ID=LAKEHOUSE_DATASET_ID)

    print(query_1)
    
    query_2 ="SELECT distinct tal.apn_impression_count,tal.target_impression_count \
FROM `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` tal \
where tal.transaction_dt='{transaction_dt}' and load_ts in (SELECT max(load_ts) \
from `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` ls \
where ls.transaction_dt=tal.transaction_dt)".format(transaction_dt=transaction_dt-timedelta(1),LAKEHOUSE_PROJECT_ID=LAKEHOUSE_PROJECT_ID,LAKEHOUSE_DATASET_ID=LAKEHOUSE_DATASET_ID)

    query_3 ="SELECT distinct tal.apn_impression_count,tal.target_impression_count \
FROM `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` tal \
where tal.transaction_dt='{transaction_dt}' and load_ts in (SELECT max(load_ts) \
from `{LAKEHOUSE_PROJECT_ID}.{LAKEHOUSE_DATASET_ID}.transaction_audit_log` ls \
where ls.transaction_dt=tal.transaction_dt)".format(transaction_dt=transaction_dt-timedelta(7), LAKEHOUSE_PROJECT_ID=LAKEHOUSE_PROJECT_ID,LAKEHOUSE_DATASET_ID=LAKEHOUSE_DATASET_ID)

    query_result_1 = execute_report_query(query_1)
    query_result_2 = execute_report_query(query_2)
    query_result_3 = execute_report_query(query_3)
    
    print(query_result_1, query_result_2, query_result_3)
    
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""

    # line 1
    email_body += """<strong>Report for today:</strong><br/>"""
    email_body += """Transaction Job report for: %s<br/>""" %(transaction_dt)
    email_body += """Number of Impressions in AppNexus API report = %s<br/>""" %(query_result_1[0])
    email_body += """Number of Impressions in fully summarized data = %s<br/>""" %(query_result_1[1])    
    difference_1 = (int(query_result_1[0]) - int(query_result_1[1]))
    email_body += """Difference of Impressions between API and files processed (should be 0): %s<br/></br>""" %(difference_1)

    # line 2
    email_body += """<strong>Report for one day prior:</strong><br/>"""
    email_body += """Transaction Job report for: %s<br/>""" %(transaction_dt - timedelta(1))
    email_body += """Number of Impressions in AppNexus API report = %s<br/>""" %(query_result_2[0])
    email_body += """Number of Impressions in fully summarized data = %s<br/>""" %(query_result_2[1])    
    difference_2 = (int(query_result_2[0]) - int(query_result_2[1]))
    email_body += """Difference of Impressions between API and files processed (should be 0): %s<br/></br>""" %(difference_2)

    # line 3
    email_body += """<strong>Report for one week prior:</strong><br/>"""
    email_body += """Transaction Job report for: %s<br/>""" %(transaction_dt - timedelta(7))
    email_body += """Number of Impressions in AppNexus API report = %s<br/>""" %(query_result_3[0])
    email_body += """Number of Impressions in fully summarized data = %s<br/>""" %(query_result_3[1])    
    difference_3 = (int(query_result_3[0]) - int(query_result_3[1]))
    email_body += """Difference of Impressions between API and files processed (should be 0): %s<br/></br>""" %(difference_3)

    if difference_1 == 0:
        email_subject = """Transaction Report for %s for %s: SUCCESS""" %(ENVIRONMENT,transaction_dt)
    else:
        email_subject = """Transaction Report for %s for %s: WARNING""" %(ENVIRONMENT,transaction_dt)

    #send report email
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# Update processed_flag in manifest_loag_stg and ctl_transaction tables
def update_processed_flag_for_current_transaction():
    transaction_dt = get_transaction_dt()    
    processed_flag = 'Y'
    ctl_statement = (
        "UPDATE `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_transaction` \
        SET processed_flag = '{processed_flag}', load_ts = '{load_ts}' \
        WHERE transaction_dt = '{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt,processed_flag=processed_flag,load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
    
    manifest_statement = (
        "UPDATE `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
        SET processed_flag = '{processed_flag}', load_ts = '{load_ts}' \
        WHERE transaction_dt = '{transaction_dt}'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,transaction_dt=transaction_dt,processed_flag=processed_flag,load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))
    
    execute_query(bq_client, ctl_statement)
    execute_query(bq_client, manifest_statement)
    return transaction_dt.strftime('%Y%m%d')

# checking manifest_log_stg table for any other transaction_dt needs to be processed, based on that DAG trigger or stop tasks executed    
def check_for_transaction_date():
    select_query = "SELECT transaction_dt \
    FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
    WHERE transaction_dt IN (SELECT MAX(transaction_dt) \
        FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
        WHERE duplicate_manifest_ind = 'N' AND processed_flag = 'N' \
        AND transaction_dt < (SELECT MAX(transaction_dt) \
            FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` \
            WHERE duplicate_manifest_ind = 'N' AND processed_flag = 'N')) \
    AND duplicate_manifest_ind = 'N' AND processed_flag = 'N' \
    GROUP BY transaction_dt".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)

    result = execute_query(bq_client, select_query)

    if result.total_rows != 0:
        dag_trigger_warning_email(dag_name=DAG_NAME, environment=ENVIRONMENT, email_type="Warning")
        return 'trigger_dag_run'
    return 'stop_dag_run_op'

# manifest_log_stg table truncate task        
bq_table_truncate = BigQueryExecuteQueryOperator(
task_id='bq_table_truncate',
destination_dataset_table="{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
sql="SELECT * FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.manifest_log_stg_copy` where 1=0".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
bigquery_conn_id='google_cloud_default',
impersonation_chain=SERVICE_ACCOUNT,
use_legacy_sql=False,
write_disposition='WRITE_TRUNCATE',
labels=GCP_COST_LABELS_BIGQUERY,
dag=dag
)

# scan the manifest files and insert into manifest stagging table
task_load_manifest_staging_table = PythonOperator(
    task_id='load_manifest_staging_table',
    python_callable=load_manifest_staging_table,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

# detect the new transaction_dt and history_dt and ignore future dates
task_load_control_table = PythonOperator(
    task_id='load_control_table',
    python_callable=load_control_table,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag)

task_detect_hist_tranaction_dt = BranchPythonOperator(
    task_id='detect_history_transaction_dt',
    python_callable=detect_history_transaction_dt,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_fetch_manifest_files_from_archival = PythonOperator(
    task_id='fetch_manifest_files_from_archival',
    python_callable=fetch_manifest_files_from_archival,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_fetch_kv_feeds_from_archival = PythonOperator(
    task_id='fetch_kv_feeds_from_archival',
    python_callable=fetch_kv_feeds_from_archival,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_fetch_standard_feeds_from_archival = PythonOperator(
    task_id='fetch_standard_feeds_from_archival',
    python_callable=fetch_standard_feeds_from_archival,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_skip_restore_archive = dummy_operator.DummyOperator(
    task_id="skip_restore_archive_tasks",
    trigger_rule="all_success",
    dag=dag
)
    
task_join = dummy_operator.DummyOperator(
    task_id="join_archive",
    trigger_rule="none_failed",
    dag=dag
)

#Avro files data load tasks
start = dummy_operator.DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    dag=dag
)

scan_manifest_files = PythonOperator(
    task_id='scan_bucket',
    python_callable=scan_bucket,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

remove_duplicate_hour_files = PythonOperator(
    task_id='remove_duplicate_hours',
    python_callable=remove_duplicate_hours,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

prepare_avro_files = PythonOperator(
    task_id='prepare_avro_files',
    python_callable=prepare_avro_files,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

load_std_avro_files_to_bq = PythonOperator(
    task_id='load_std_feed_avro_files_to_bq',
    python_callable=load_std_feed_avro_files_to_bq,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

run_query_placement_tag = PythonOperator(
    task_id='run_query_placement_tag',
    python_callable=run_query_placement_tag,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
) 

load_kv_avro_files_to_bq = PythonOperator(
    task_id='load_kv_feed_avro_files_to_bq',
    python_callable=load_kv_feed_avro_files_to_bq,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

archive_duplicate_files = PythonOperator(
    task_id='archive_duplicate_files',
    python_callable=archive_duplicate_files,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

check_valid_avro_files = PythonOperator(
    task_id='check_valid_avro_files',
    python_callable=check_valid_avro_files,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

archive_valid_manifest_files = PythonOperator(
    task_id='archive_valid_manifest_files',
    python_callable=archive_valid_manifest_files,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

archive_valid_avro_files = PythonOperator(
    task_id='archive_valid_avro_files',
    python_callable=archive_valid_avro_files,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

avro_load_complete = dummy_operator.DummyOperator(
    task_id='avro_load_complete',
    trigger_rule='all_success',
    dag=dag
)

# define API the first task
api_auth = PythonOperator(
    task_id='api_authentication',
    python_callable=api_auth,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

api_post = PythonOperator(
    task_id='api_post',
    python_callable=api_post,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

download_file = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

load_bq = PythonOperator(
    task_id='load_bq',
    python_callable=load_bq,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

execute_apn_to_bq = BigQueryExecuteQueryOperator(
    task_id='execute_apn_to_bq',
    sql="{{ task_instance.xcom_pull(task_ids='load_bq', key='sql_key') }}",
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    write_disposition="WRITE_APPEND",
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

api_load_complete = dummy_operator.DummyOperator(
    task_id='api_load_complete',
    trigger_rule='all_success',
    dag=dag
)

transaction_report_email = PythonOperator(
    task_id='transaction_report_email',
    python_callable=transaction_report_email,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_update_processed_flag = PythonOperator(
    task_id='update_processed_flag_for_current_transaction',
    python_callable=update_processed_flag_for_current_transaction,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

decide_dag_rerun = BranchPythonOperator(
    task_id='decide_dag_rerun_op',
    python_callable=check_for_transaction_date,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

trigger_dag_run_again = TriggerDagRunOperator(
    task_id="trigger_dag_run",
    trigger_dag_id=DAG_NAME,
    wait_for_completion=False,
    dag=dag
)

stop_dag_runs = dummy_operator.DummyOperator(
    task_id="stop_dag_run_op",
    trigger_rule="all_success",
    dag=dag
)

end = dummy_operator.DummyOperator(
    task_id='end',
    trigger_rule='none_failed',
    dag=dag
)

# Tasks to handle history and current transaction_dt manifest and avro feed data
start >> bq_table_truncate >> task_load_manifest_staging_table >> task_load_control_table >> task_detect_hist_tranaction_dt >> task_fetch_manifest_files_from_archival >> [task_fetch_kv_feeds_from_archival, task_fetch_standard_feeds_from_archival] >> task_join
task_detect_hist_tranaction_dt >> task_skip_restore_archive >> task_join

task_join >> scan_manifest_files

# Avro files process tasks
scan_manifest_files >> remove_duplicate_hour_files >> prepare_avro_files 
prepare_avro_files >> [archive_duplicate_files, check_valid_avro_files]
check_valid_avro_files >> [load_std_avro_files_to_bq, load_kv_avro_files_to_bq] >> run_query_placement_tag >> avro_load_complete

# APN API tasks
scan_manifest_files >> delete_stg_audit_table >> api_auth >> api_post >> download_file >> load_bq >> execute_apn_to_bq >> api_load_complete

# Transform and load SQL & email tasks
[avro_load_complete,api_load_complete] >> load_into_wads_adserver_trans_init >> wads_adserver_trans_init_row_and_impression_counts_added_to_audit_stg >> check_dupes_wads_adserver_trans_init
check_dupes_wads_adserver_trans_init >> count_duplicate_rows_in_wads_adserver_trans_init >> insert_into_wads_adserver_trans >> [update_action_cd_mapped,run_query_city] >> update_OTH_action_cd 
update_OTH_action_cd >> insert_existing_transaction_dt_into_adserver_transaction_old_rows_removed 
insert_existing_transaction_dt_into_adserver_transaction_old_rows_removed >> [update_audit_rows_removed_from_adserver_transaction_old_rows_removed,run_query_oth]  >> delete_adserver_transaction_transaction_dt_in_wads_adserver_trans
delete_adserver_transaction_transaction_dt_in_wads_adserver_trans >> insert_adserver_transaction >> target_row_and_impression_counts_added_to_audit_stg 
target_row_and_impression_counts_added_to_audit_stg >> row_count_diff_stg_to_tgt_added_to_audit_stg 
row_count_diff_stg_to_tgt_added_to_audit_stg >> insert_into_target_transaction_audit_log >> transaction_report_email

transaction_report_email >> task_update_processed_flag >> archive_valid_avro_files >> archive_valid_manifest_files >> decide_dag_rerun >> [trigger_dag_run_again, stop_dag_runs] >> end