# Import library
import io
import os
import tarfile
from datetime import timedelta
import subprocess
import pytz
import pendulum
from dateutil import parser

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCheckOperator
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
#from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from pathlib import Path
import csv
import shutil
from datetime import datetime

# Config variables
ENVIRONMENT = os.getenv("env").upper()
REFERENCE_PARAMS_PATH = "/home/airflow/gcs/dags/ads-pcc-imprsns"  # os.environ.get('AIRFLOW_HOME')

DAG_NAME = "ads_pcc_imprsns_bq_to_bq_data_extract"

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

LOCAL_TZ = pendulum.timezone("America/Chicago")

# CURRENT_TIME = datetime.now()
CURRENT_TIME = datetime.now(pytz.timezone('US/Central'))
CURRENT_TIME_FORMAT = CURRENT_TIME.strftime("%Y%m%d-%H%M")
FILE_NAME = f"PCC_IMPRESSIONS_{CURRENT_TIME_FORMAT}"

# Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        print(my_config_dict)
        return my_config_dict


# read values from yaml and set vars
my_config_values = read_parameters_from_file(REFERENCE_PARAMS_PATH + '/pcc_imprsns/config_pcc_imprsns.yaml',
                                             ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
# INGRESS_PROJECT_ID = my_config_values['ingress_project_id']
EGRESS_PROJECT_ID = my_config_values['egress_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
# INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
# INGRESS_EFG_BUCKET_NAME = my_config_values['ingress_efg_bucket_name']
EGRESS_EFG_BUCKET_NAME = my_config_values['egress_efg_bucket_name']
PIPELINE_BUCKET_NAME = my_config_values['pipeline_bucket_name']
ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']
NBR_OF_DAYS_BACK_TO_CHECK = my_config_values['nbr_of_days_back_to_check']
ADS_TRANSACTION_DAG_NAME = my_config_values['ads_transaction_dag_name']
ADS_REFERENCE_HOURS_BACK_FOR_SENSOR = my_config_values['ads_reference_hours_back_for_sensor']
ADS_REFERENCE_DAYS_BACK_FOR_SENSOR = my_config_values['ads_reference_days_back_for_sensor']
MOVE_FILE_TO_EFG_EGRESS_BUCKET = my_config_values['move_file_to_efg_egress_bucket']
CC_RECIPIENT_EMAIL = my_config_values['cc_recipient_email']
RESELLER_NAME = my_config_values['reseller_name']
NUMBER_OF_DAYS_TO_KEEP_IN_AUDIT_TBL = my_config_values['number_of_days_to_keep_in_audit_tbl']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime(parser.parse(START_DATE).year, parser.parse(START_DATE).month, parser.parse(START_DATE).day, tzinfo=LOCAL_TZ), # START_DATE Format:"2023-03-21"
    'email': FAILURE_RECIPIENT_EMAIL, # ['surendra.gangavaram.ctr@sabre.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Labels
GCP_COST_LABELS_BIGQUERY = {
    "business_unit": "ts",
    "contact_email": "adserverappstatus__sabre_com",
    "owner": "sg0441328",
    "name": "sab-da-ads-pcc-imprsns",   # sab-da-(product_name)
    "business_service": "ads",  # This is business service asset tag, so will always be "ads" for AdMedia.
    "application_service": "pcc-impressions-extract-gcp",  # This is technical service asset tag.
    "environment": ENVIRONMENT.lower(),
    "logical_environment": ENVIRONMENT.lower(),
    "query_location": "bigquery_operator"
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
    description='ADS AdMedia Reporting-PCC Impressions Extract GCP job',  # Technical service name.
    catchup=True,
    default_args=default_args,
    schedule_interval='0 3 * * *', # this is Central time due to tzinfo above  #None # SCHEDULE_INTERVAL # '0 16 * * *'
    max_active_runs=1,
    template_searchpath=REFERENCE_PARAMS_PATH + '/pcc_imprsns')

# Impersonating BigQuery client
def get_bq_impersonated_client(target_service_account: str, target_project: str):
    return bigquery.Client(project=target_project,
                           credentials=getImpersonatedCredentials(target_scopes, target_service_account,
                                                                  target_project))

# Impersonating GCS storage client
def get_storage_impersonated_client(target_service_account: str, target_project: str):
    from google.cloud import storage

    return storage.Client(project=target_project,
                          credentials=getImpersonatedCredentials(target_scopes, target_service_account, target_project))

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

# Storage client with impersonated service client (between the projects)
storage_client_impersonated = get_storage_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT, LAKEHOUSE_PROJECT_ID)

# send warning/failure email function
def send_email(recipient_email, subject, body, cc=None):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content=body, mime_charset='utf-8', cc=cc)

#this function will disable DAG after failure
def pause_dag(context):
    cmd = ['airflow','pause',DAG_NAME]
    subprocess.call(cmd)

def task_failure_alert_and_pause_dag_transaction(context):   
    task_name = context['task_instance_key_str'].split('__')[1]
    print("TABLE_NAME-1:",task_name)
    table_name = task_name[:-11]
    print("TABLE_NAME-2:",table_name)
    email_type = 'Failure'
    email_subject = """%s %s for ads-pcc-imprsns ADS:PCC-IMPRESSIONS-EXTRACT-GCP %s - Task %s: Failed""" %(ENVIRONMENT, email_type, DAG_NAME, task_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>: Upstream transaction data source failed a validation check for yesterday’s data load.</br></br>"""
    email_body += """<strong>Actions</strong>:</br></br>"""
    email_body += """<ol>
                        <li><strong>UPSTREAM DAG</strong> %s:
                            <ul>
                                <li>Fix issue with yesterday’s run of upstream source DAG %s, so it can run and complete its processing.</li>
                                <li>Once the issue is corrected & the upstream DAG completed, ensure the status of yesterday’s <strong>scheduled</strong> run of this DAG is successful.</li>
                                <li>If DAG was manually run to fix issue, mark yesterday’s <strong>scheduled</strong> run successful in Airflow, if it shows a red/“failed” status.</li>
                            </ul>                                  
                        <li><strong>CURRENT DAG</strong> %s:
                            <ul>
                                <li>Reactivate paused DAG in Airflow by changing it from “Off” back to “On”.</li>
                                <li>Clear failed task in current <strong>scheduled</strong> DAG, %s, so this DAG will restart where it left off.</li>
                                <li>Ensure this DAG completes successfully, placing extract named “%s_PCC_IMPRESSIONS_%s-<i>hhmi</i>.zip” (where <i>hhmi</i> = hour and minute from job restart) in GCS bucket: sab-%s-dap-efg-egress/ads-pcc-imprsns</li>
                            </ul>
                        </li>
                    </ol>"""%(ADS_TRANSACTION_DAG_NAME,ADS_TRANSACTION_DAG_NAME,DAG_NAME,DAG_NAME,ENVIRONMENT.title(),CURRENT_TIME.strftime("%Y%m%d"),ENVIRONMENT.lower())
    send_email(FAILURE_RECIPIENT_EMAIL, email_subject, email_body, cc=CC_RECIPIENT_EMAIL)
    # pause the dag
    pause_dag(context)

def task_failure_alert_and_pause_dag_reference_sensor(context):    
    task_name = context['task_instance_key_str'].split('__')[1]
    print("TASK_NAME:",task_name)
    ref_dag_name = task_name[:-20]
    print("DAG_NAME:",ref_dag_name)
    email_type = 'Failure'
    email_subject = """%s %s for ads-pcc-imprsns ADS:PCC-IMPRESSIONS-EXTRACT-GCP %s - Task %s: Failed""" %(ENVIRONMENT, email_type, DAG_NAME, task_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>: Upstream reference data source failed a validation check for yesterday’s data load.</br></br>"""
    email_body += """<strong>Actions</strong>:</br></br>"""
    email_body += """<ol>
                        <li><strong>UPSTREAM DAG</strong> %s:
                            <ul>
                                <li>Fix issue with yesterday’s run of upstream source DAG %s, so it can run and complete its processing.</li>
                                <li>Once the issue is corrected & the upstream DAG completed, ensure the status of yesterday’s <strong>scheduled</strong> run of this DAG is successful.</li>
                                <li>If DAG was manually run to fix issue, mark yesterday’s <strong>scheduled</strong> run successful in Airflow, if it shows a red/“failed” status.</li>
                            </ul>                                  
                        <li><strong>CURRENT DAG</strong> %s:
                            <ul>
                                <li>Reactivate paused DAG in Airflow by changing it from “Off” back to “On”.</li>
                                <li>Clear failed task in current <strong>scheduled</strong> DAG, %s, so this DAG will restart where it left off.</li>
                                <li>Ensure this DAG completes successfully, placing extract named “%s_PCC_IMPRESSIONS_%s-<i>hhmi</i>.zip” (where <i>hhmi</i> = hour and minute from job restart) in GCS bucket: sab-%s-dap-efg-egress/ads-pcc-imprsns</li>
                            </ul>
                        </li>
                    </ol>"""%(ref_dag_name,ref_dag_name,DAG_NAME,DAG_NAME,ENVIRONMENT.title(),CURRENT_TIME.strftime("%Y%m%d"),ENVIRONMENT.lower())
    send_email(FAILURE_RECIPIENT_EMAIL, email_subject, email_body, cc=CC_RECIPIENT_EMAIL)
    # pause the dag
    pause_dag(context)

def send_warning_email_epr_header_mdm_travel_agency_view(task_name, table_name, lakehouse_dataset_name, upstream_dag_name, column_name):    
    email_type = 'Warning'
    email_subject = """%s %s for ads-pcc-imprsns ADS:PCC-IMPRESSIONS-EXTRACT-GCP %s""" %(ENVIRONMENT, email_type, DAG_NAME)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: Data source table %s.%s was not updated yesterday with latest updates; however, today’s PCC Impressions extract was sent using existing data.</br></br>"""%(lakehouse_dataset_name,table_name)
    email_body += """<strong>SQL Used in Task “%s”:</strong></br>"""%(task_name)
    email_body += """SELECT COUNT(*) records_count FROM %s.%s.%s WHERE SAFE_CAST(%s AS DATE) > CURRENT_DATE-%s </br></br>"""%(LAKEHOUSE_PROJECT_ID,lakehouse_dataset_name,table_name,column_name,NBR_OF_DAYS_BACK_TO_CHECK)
    email_body += """<strong>Action</strong>: Check %s DAG to see if failure is pending.  If so, contact DNAGCPPlatformSupport@sabre.com to verify resolution is in progress."""%(upstream_dag_name)
    
    send_email(WARNING_RECIPIENT_EMAIL, email_subject, email_body)

def send_failure_email_wads_all_extract_columns(context):
    day_before_yesterday_dt = datetime.now(pytz.timezone('US/Central')) - timedelta(days=2)
    print("DAY_BEFORE_YESTERDAY:",day_before_yesterday_dt.strftime("%Y-%m-%d"))
    yesterday_dt = datetime.now(pytz.timezone('US/Central')) - timedelta(days=1)
    print("YESTERDAY_DATE:",yesterday_dt.strftime("%Y-%m-%d"))
    task_name = context['task_instance_key_str'].split('__')[1]
    print("TASK_NAME:",task_name)
    email_type = 'Failure'
    email_subject = """%s %s for ads-pcc-imprsns ADS:PCC-IMPRESSIONS-EXTRACT-GCP %s - Task %s: Failed""" %(ENVIRONMENT, email_type, DAG_NAME, task_name)
    
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Failure</strong>:</br></br>"""
    email_body += """Today's PCC Impressions extract is unable to be sent today, because there is no transaction/impression data to be extracted, 
                    so the outgoing file to %s would be empty. The transaction data that should be included in the extract is 
                    from two days ago, %s, and should have been loaded yesterday, %s, into GCP %s promospot_ads.adserver_transaction with DAG ads_gcs_to_bq_avro_data_load.</br></br>"""%(RESELLER_NAME,day_before_yesterday_dt.strftime("%Y-%m-%d"),yesterday_dt.strftime("%Y-%m-%d"),ENVIRONMENT.title())
    email_body += """<strong>Actions</strong>:</br></br>"""
    email_body += """1. Development & Business teams:  Is there some reason why there would be no impression data for two days ago for %s?  
                      Were there any known issues with SRW 360 or AppNexus/Xandr yesterday that would prevent impressions from being sent in the daily file?</br></br>"""%(RESELLER_NAME)
    email_body += """2. D&A SREs: We will advise if the DAG needs to be rerun or if any other support is needed."""
    
    send_email(FAILURE_RECIPIENT_EMAIL, email_subject, email_body, cc=CC_RECIPIENT_EMAIL)

def check_epr_header_and_mdm_travel_agency_view_data_and_email(**kwargs):
    task_name = kwargs['task_instance_key_str'].split('__')[1]
    table_name = kwargs["table_name"]
    lakehouse_dataset_name = kwargs["lakehouse_dataset_name"]
    upstream_dag_name = kwargs["upstream_dag_name"]
    column_name = kwargs["column_name"]
    sql_queries = {"epr_header":f"SELECT COUNT(*) records_count \
FROM {LAKEHOUSE_PROJECT_ID}.epr.epr_header WHERE SAFE_CAST(ingest_ts AS DATE) > CURRENT_DATE-{NBR_OF_DAYS_BACK_TO_CHECK}",
"mdm_travel_agency_view":f"SELECT COUNT(*) records_count \
FROM {LAKEHOUSE_PROJECT_ID}.reference_data.mdm_travel_agency_view WHERE SAFE_CAST(Record_load_ts AS DATE) > CURRENT_DATE-{NBR_OF_DAYS_BACK_TO_CHECK}"
}
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    records_count_query = sql_queries[table_name]
    print("query:" + records_count_query)
    records_count_query_result = bq_client.query(records_count_query, location="US", job_config=job_config)
    results = records_count_query_result.result()
    for result in results:
        if result.records_count == 0:
            send_warning_email_epr_header_mdm_travel_agency_view(task_name, table_name, lakehouse_dataset_name, upstream_dag_name, column_name)
            raise AirflowSkipException 

def scan_pipeline_bucket_and_delete_work_files():
    bucket = storage_client_impersonated.bucket(PIPELINE_BUCKET_NAME)
    blobs = bucket.list_blobs()
    for blob in blobs:
        print(f'Deleting file {blob.name}')
        blob.delete()
    # bucket.delete_blobs(bucket.list_blobs())

def decide_if_copy_pcc_imprsns_to_EFG():
    if MOVE_FILE_TO_EFG_EGRESS_BUCKET == 'Yes':
        return "copy_pcc_imprsns_to_EFG"
    else:
        return "do_not_copy_pcc_imprsns_to_EFG"

# def check_wads_all_extract_columns_records():
#     query = "select count(*) as records_count from {PIPELINE_PROJECT_ID}.ads_pcc_imprsns_stg.wads_all_extract_columns".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID)
#     client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
#     query_job = client.query(
#         query
#     )
#     results = query_job.result()
#     for result in results:
#         if result.records_count == 0:
#             send_failure_email_wads_all_extract_columns()
#             raise AirflowException

start = DummyOperator(
    task_id='start_pipeline',
    trigger_rule='all_success',
    dag=dag
)

# task_delete_bucket_files = GCSDeleteObjectsOperator(
#     task_id="delete_bucket_files",
#     bucket_name="sab-dev-dap-ads-pcc-imprsns-pcc_imprsns_stg",
#     prefix= "Test", # Requires folder in bucket, works whether files exist or not
#     #objects = "gs://sab-dev-dap-ads-pcc-imprsns-pcc_imprsns_stg/Test",
#     impersonation_chain=SERVICE_ACCOUNT,
#     labels=GCP_COST_LABELS_PYTHON_API,
#     dag=dag
# )

transaction_audit_log_data_exist = BigQueryCheckOperator(
    task_id="transaction_audit_log_data_exist",
    sql="""SELECT count(*) FROM `%s.promospots_ads.transaction_audit_log` WHERE target_rows_loaded_count > 0 AND transaction_dt = '%s';""" %(LAKEHOUSE_PROJECT_ID,'{{ yesterday_ds }}'),
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    on_failure_callback=task_failure_alert_and_pause_dag_transaction,
    email_on_failure=False,
    impersonation_chain=SERVICE_ACCOUNT,
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

ads_reference_gcs_to_bq_data_load_status_check_sensor = ExternalTaskSensor(
    task_id='ads_reference_gcs_to_bq_data_load_status_check_sensor',
    external_dag_id='ads_reference_gcs_to_bq_data_load',
    execution_delta=timedelta(days=ADS_REFERENCE_DAYS_BACK_FOR_SENSOR,hours=ADS_REFERENCE_HOURS_BACK_FOR_SENSOR,minutes=0), # days=0 (days back), hours=15 (hours back)
    timeout=60,
    on_failure_callback=task_failure_alert_and_pause_dag_reference_sensor,
    email_on_failure=False,
     dag=dag
)

check_epr_header_data_and_email = PythonOperator(
    task_id='check_epr_header_data_and_email',
    dag=dag,
    op_kwargs={"table_name":"epr_header", "lakehouse_dataset_name":"epr", "upstream_dag_name":"epr_hdr_kwd_oac_daily", "column_name":"ingest_ts"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_epr_header_and_mdm_travel_agency_view_data_and_email
)

check_mdm_travel_agency_view_data_and_email = PythonOperator(
    task_id='check_mdm_travel_agency_view_data_and_email',
    dag=dag,
    op_kwargs={"table_name":"mdm_travel_agency_view", "lakehouse_dataset_name":"reference_data", "upstream_dag_name":"MDM_Data_Ingestion_to_BigQuery", "column_name":"Record_Load_Ts"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_epr_header_and_mdm_travel_agency_view_data_and_email
)

task_wads_insertion_order_campaign = BigQueryExecuteQueryOperator(
    task_id='wads_insertion_order_campaign',
    sql='01_insert_wads_insertion_order_campaign.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID, "RESELLER_NAME": RESELLER_NAME},
    trigger_rule='none_failed',
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
task_wads_adserver_transaction_counts = BigQueryExecuteQueryOperator(
    task_id='wads_adserver_transaction_counts',
    sql='02_insert_wads_adserver_transaction_counts.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
task_wads_all_extract_columns = BigQueryExecuteQueryOperator(
    task_id='wads_all_extract_columns',
    sql='03a_insert_wads_all_extract_columns.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
task_wads_epr_header = BigQueryExecuteQueryOperator(
    task_id='wads_epr_header',
    sql='03b_insert_wads_epr_header.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

task_update_wads_all_extract_columns = BigQueryExecuteQueryOperator(
    task_id='update_wads_all_extract_columns',
    sql='04_update_wads_all_extract_columns.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

task_check_wads_all_extract_columns_records = BigQueryCheckOperator(
    task_id="check_wads_all_extract_columns_records",
    sql="""SELECT count(*) FROM `%s.ads_pcc_imprsns_stg.wads_all_extract_columns`;""" %(PIPELINE_PROJECT_ID),
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    on_failure_callback=send_failure_email_wads_all_extract_columns,
    email_on_failure=False,
    impersonation_chain=SERVICE_ACCOUNT,
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
)

task_insert_wads_pcc_impressions_extract_audit = BigQueryExecuteQueryOperator(
    task_id='insert_wads_pcc_impressions_extract_audit',
    sql='05_insert_wads_pcc_impressions_extract_audit.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "RESELLER_NAME": RESELLER_NAME, "JOB_RUN_TS": CURRENT_TIME, "FILE_NAME": FILE_NAME},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

def get_file_name():
    query = "select distinct file_name from {PIPELINE_PROJECT_ID}.ads_pcc_imprsns_stg.wads_pcc_impressions_extract_audit where job_run_ts in ( \
        select max(job_run_ts) from {PIPELINE_PROJECT_ID}.ads_pcc_imprsns_stg.wads_pcc_impressions_extract_audit)".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID)
    #result = execute_query(query)
    client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
    query_job = client.query(
        query
    )
    results = query_job.result()
    for row in results:
        return row.file_name
    
FILE_NAME_WITH_TIME_STAMP = get_file_name()

task_scan_pipeline_bucket_and_delete_work_files = PythonOperator(
    task_id='scan_pipeline_bucket_and_delete_work_files',
    python_callable=scan_pipeline_bucket_and_delete_work_files,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_export_wads_all_extract_columns_to_gcs = BigQueryToGCSOperator(
    task_id='export_wads_all_extract_columns_to_gcs',
    source_project_dataset_table=f"{PIPELINE_PROJECT_ID}.ads_pcc_imprsns_stg.wads_all_extract_columns",
    destination_cloud_storage_uris=f"gs://{PIPELINE_BUCKET_NAME}/{FILE_NAME_WITH_TIME_STAMP}",
    print_header=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    # bigquery_conn_id='google_cloud_default',
    export_format="CSV",
    field_delimiter="|",
    compression="NONE",
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag
    )

task_decide_if_copy_pcc_imprsns_to_EFG = BranchPythonOperator(
    task_id='decide_if_copy_pcc_imprsns_to_EFG',
    python_callable=decide_if_copy_pcc_imprsns_to_EFG,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_copy_pcc_imprsns_to_EFG = GCSToGCSOperator(
    task_id="copy_pcc_imprsns_to_EFG",
    source_bucket=f"{PIPELINE_BUCKET_NAME}",
    source_object=f"{FILE_NAME_WITH_TIME_STAMP}",
    destination_bucket=f"{EGRESS_EFG_BUCKET_NAME}",
    destination_object=f"ads-pcc-imprsns/{FILE_NAME_WITH_TIME_STAMP}",
    move_object=False, # 'True' flag will move the object, 'False' flag will copy the object and this parameter is optional
    labels=GCP_COST_LABELS_PYTHON_API,
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag
)

task_do_not_copy_pcc_imprsns_to_EFG = DummyOperator(
    task_id="do_not_copy_pcc_imprsns_to_EFG",
    trigger_rule="all_success",
    dag=dag
)

task_move_file_from_pipeline_to_archive = GCSToGCSOperator(
    task_id="move_file_from_pipeline_to_archive",
    source_bucket=f"{PIPELINE_BUCKET_NAME}",
    source_object=f"{FILE_NAME_WITH_TIME_STAMP}",
    destination_bucket=f"{ARCHIVE_BUCKET_NAME}",
    # destination_object='',
    move_object=True, # 'True' flag will move the object, 'False' flag will copy the object and this parameter is optional
    labels=GCP_COST_LABELS_PYTHON_API,
    impersonation_chain=SERVICE_ACCOUNT,
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

# task_temp_delete_efg_bucket_files = GCSDeleteObjectsOperator(
#     task_id="temp_delete_efg_bucket_files",
#     bucket_name=f"{ARCHIVE_BUCKET_NAME}",
#     prefix= "ads-pcc-imprsns/PCC_IMPRESSIONS_20230228-0123", # Requires folder in bucket, works whether files exist or not
#     # objects = "ads-pcc-imprsns/PCC_IMPRESSIONS_*",
#     impersonation_chain=SERVICE_ACCOUNT,
#     labels=GCP_COST_LABELS_PYTHON_API,
#     dag=dag
# )

task_insert_pcc_impressions_extract_audit = BigQueryExecuteQueryOperator(
    task_id='insert_pcc_impressions_extract_audit',
    sql='06_insert_pcc_impressions_extract_audit.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

task_delete_pcc_impressions_extract_audit = BigQueryExecuteQueryOperator(
    task_id='delete_pcc_impressions_extract_audit',
    sql='07_delete_pcc_impressions_extract_audit.sql',
    use_legacy_sql=False,
    # bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID, "NUMBER_OF_DAYS_TO_KEEP_IN_AUDIT_TBL": NUMBER_OF_DAYS_TO_KEEP_IN_AUDIT_TBL},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

end = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='none_failed_or_skipped',
    dag=dag
)

# start >> task_delete_bucket_files >> transaction_audit_log_data_exist >> ads_reference_gcs_to_bq_data_load_status_check_sensor >> [check_epr_header_data_and_email,check_mdm_travel_agency_view_data_and_email] >> task_wads_insertion_order_campaign
start >> transaction_audit_log_data_exist >> ads_reference_gcs_to_bq_data_load_status_check_sensor >> [check_epr_header_data_and_email,check_mdm_travel_agency_view_data_and_email] >> task_wads_insertion_order_campaign
task_wads_insertion_order_campaign >> task_wads_adserver_transaction_counts >> [task_wads_all_extract_columns, task_wads_epr_header] >> task_update_wads_all_extract_columns >> task_check_wads_all_extract_columns_records >> task_insert_wads_pcc_impressions_extract_audit >> task_scan_pipeline_bucket_and_delete_work_files 
task_scan_pipeline_bucket_and_delete_work_files >> task_export_wads_all_extract_columns_to_gcs >> task_decide_if_copy_pcc_imprsns_to_EFG
task_decide_if_copy_pcc_imprsns_to_EFG >> [task_copy_pcc_imprsns_to_EFG,task_do_not_copy_pcc_imprsns_to_EFG] >> task_move_file_from_pipeline_to_archive >> task_insert_pcc_impressions_extract_audit
task_insert_pcc_impressions_extract_audit >> task_delete_pcc_impressions_extract_audit >> end