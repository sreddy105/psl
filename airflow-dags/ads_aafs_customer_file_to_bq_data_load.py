# Import library
import os
import airflow
import re
import io
import logging
import time
from io import StringIO
import csv
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
from airflow import  settings
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Config variables
ENVIRONMENT = os.getenv("env").upper()
AAFS_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')

DAG_NAME="ads_aafs_customer_file_to_bq_data_load"

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
INGRESS_EFG_BUCKET_NAME = my_config_values['ingress_efg_bucket_name']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']
PIPELINE_BUCKET_NAME = my_config_values['pipeline_bucket_name']
AMEX_RECIPIENT_EMAIL = my_config_values['amex_recipient_email']
MAX_PCC_VENDORS = my_config_values['max_pcc_vendors']
UPDATE_EXTRACT_TABLE = my_config_values['update_extract_table']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d'),#'2022-01-12'
    'email': FAILURE_RECIPIENT_EMAIL,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=20)
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
    description='ADS AdMedia Reporting-Agency Ad Filtering Service-Load GCP (AAFS Load) job',
    catchup=False,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,  # SCHEDULE_INTERVAL, None, "30 15 * * *" (in UTC time) = 10:30 AM CST daily, may use monthly schedule in the future  "*/31 15 5 * *" (in UTC time) = 10:31 AM CST on 5th of each month 
    max_active_runs= 1,
    template_searchpath=AAFS_PARAMS_PATH + '/ads-aafs/aafs')
    
#Impersonating BigqQuery client          
def get_bq_impersonated_client(target_service_account: str, target_project: str):
                
        return bigquery.Client(project = target_project,
                                                      credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))

#Impersonating gcs storage client                                                       
def get_storage_impersonated_client(target_service_account: str, target_project: str):
        from google.cloud import storage
        
        return storage.Client(project = target_project,
                                                      credentials = getImpersonatedCredentials(target_scopes, target_service_account, target_project))    

#Get service account impersonated credentials
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

#Storage client with impersonated service client (between the projects)
storage_client_impersonated = get_storage_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)

# send warning/failure email function
def send_warning_email(recipient_email, subject, body, cc=None):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content = body, mime_charset='utf-8', cc=cc)

# send skipping exception email when there are no incoming customer files to load
def raise_skipping_exception_email(environment, email_type, dag_name, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Warning</strong>: No incoming AAFS customer files found to load.</br></br>"""
    
    send_warning_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# send success email after load completion
def outer_success_email(environment, email_type, dag_name, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Success</strong>: The incoming AAFS customer file processing has completed successfully.</br></br>"""
    
    send_warning_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# sent restart DAG email for another load if more than one customer file received
def outer_success_email_with_restart(environment, email_type, dag_name, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP"):
    email_subject = """%s %s for %s %s %s""" %(environment, email_type, product, assets_tag, dag_name)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """<strong>Success</strong>: The incoming AAFS customer file processing has completed successfully.</br></br>"""
    email_body += """<strong>FYI Only: No action is required.</strong></br></br>"""
    email_body += """This AdMedia DAG, ads_aafs_customer_file_to_bq_data_load with asset tag ADS:AAFS-LOAD-GCP, 
    has detected the existence of an additional incoming customer file in its EFG ingress bucket, 
    which also needs to be processed. As a result, the last step of this DAG will trigger another instance of this same DAG to start <u><i>after</i></u> this DAG 
    has finished; the new instance will process another customer file. </br></br>"""
    email_body += """<strong>Note</strong>: In this job’s DAG description section, the “max_active_runs” option is set to 1 
    to enforce that only one instance of the DAG will run at a time. </br></br>"""
    
    send_warning_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body)

# send email when inner file, file type is invalid
def inner_file_type_warning_email(environment, email_type, file_name, product, customer_id):
    email_subject = """%s %s for %s""" %(environment, email_type, product)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """Greetings,<br /><br />"""
    email_body += """<strong>Warning</strong>: The AAFS opt-in list with filename %s could not be loaded.</br></br>""" %(file_name.split("/",1)[1])
    email_body += """<strong>Action</strong>: Please review the error log details below and make corrections. Then, resend the corrected opt-in list file. 
    For more information, please contact %s.</br></br>""" %(WARNING_RECIPIENT_EMAIL)
    email_body += """Invalid file type (%s) - check file specification and correct the file extension.</br></br>""" %(file_name.split(".")[-1])
    
    email_body += """<br>Thank you,<br />AdMedia Support<br />"""
    
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL) 

# send email when inner file, file name is invalid    
def inner_file_name_warning_email(environment, email_type, file_name, product, customer_id):
    email_subject = """%s %s for %s""" %(environment, email_type, product)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += """Greetings,<br /><br />"""
    email_body += """<strong>Warning</strong>: The AAFS opt-in list with filename %s could not be loaded.</br></br>""" %(file_name.split("/",1)[1])
    email_body += """<strong>Action</strong>: Please review the error log details below and make corrections. Then, 
    resend the corrected opt-in list file. For more information, please contact %s.</br></br>""" %(WARNING_RECIPIENT_EMAIL)
    email_body += """Invalid file name - check file specification and correct the file name.</br></br>"""
    
    email_body += """<br>Thank you,<br />AdMedia Support<br />"""
    
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL) 

# send header and data validatin error email
def inner_file_contents_error_email(header_row_error, data_row_error, file_name, customer_id, empty_file_flag=None):
    body = """ """
    body += """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """Greetings,<br /><br />"""
    body += """<strong>Warning:</strong> The AAFS opt-in list with filename %s could not be loaded.<br /><br />""" %(file_name)
    body += """<strong>Action:</strong> Please review the error log details below and make corrections. Then, resend the corrected opt-in list file. For more information, please contact %s. <br /><br />""" %(WARNING_RECIPIENT_EMAIL)
    if empty_file_flag:
        for error in header_row_error:
            body += """%s <br />""" % (error)
        body += """<br /> """
    else:
        if header_row_error:
            body += """<strong>File Header Row Errors</strong> <br />"""
            for error in header_row_error:
                body += """%s <br />""" % (error)
            body += """<br /> """
        if data_row_error:
            body += """<strong>File Data Row Errors</strong> <br />"""
            for error in data_row_error:
                body += """%s <br /> """ % (error)
            body += """<br /> """
    body += """<br>Thank you,<br />AdMedia Support<br />"""
    
    email_subject = """%s %s for %s""" %(ENVIRONMENT, "Warning", "AAFS Opt-in List Load")
    
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL) 

# scan the ingress bucket for incoming customer files
def scan_bucket_for_file(prefix, regex):
    blobs = storage_client_impersonated.list_blobs(INGRESS_EFG_BUCKET_NAME, prefix=prefix)
    IngressFilesList = []
    for blob in blobs:
        if regex in str(blob.name):
            last_char_index = str(blob.name).rfind(".")
            fileName = str(blob.name)[:last_char_index]#.split(".")[:-1]
            IngressFilesList.append(fileName)
    if IngressFilesList:
        return IngressFilesList
    return None

# skip dag run function
def skip_dag_run():
    raise_skipping_exception_email(environment=ENVIRONMENT, email_type="Warning", dag_name=DAG_NAME)

# check whether incoming customer file has related "done" file  
def check_outer_customer_zip_file(**kwargs):
    prefix = "ads-aafs/"
    regex = "done"
    IngressFilesList = scan_bucket_for_file(prefix, regex)
    print("INGRESS FILES LIST:",IngressFilesList)
    if IngressFilesList is None:
        return 'skip_dag_run'
    return "DummyForward" 

# prepare ctl_aafs_customer_files table fields for insert sql statement for inner/outer file
def prepare_ctl_customer_aafs(file_names_list:list, IsInner=False, **kwargs):
    values = []
    if IsInner:
        for file_path in file_names_list:
            file_name = file_path
            outer_file_flag = 'N'
            processed_flag = 'N/A'
            customer_name = file_name.split("_")[1]
            customer_id = 1 if customer_name == 'amex' else 0
            row = f"('{file_name}', '{outer_file_flag}', '{processed_flag}', {customer_id})"
            values.append(row)
    else:      
        for file_path in file_names_list:
            file_name = file_path
            outer_file_flag = 'Y'
            processed_flag = 'N'
            customer_name = file_name.split("_")[1]
            customer_id = 1 if customer_name == 'amex' else 0
            row = f"('{file_name}', '{outer_file_flag}', '{processed_flag}', {customer_id})"
            values.append(row)
        
    return values, customer_id

# scan bucket, read and prepare sql statement and insert into ctl_aafs_customer_files table
def load_zip_into_ctl_customer_aafs_table(**kwargs):
    prefix = "ads-aafs/"
    regex = "done"
    file_row_values = scan_bucket_for_file(prefix, regex)
    file_row_values,customer_id = prepare_ctl_customer_aafs(file_row_values)
    insert_file_names_sql = f"INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files` VALUES " + ",".join(
        file_row_values)
    print(insert_file_names_sql)

    insert_client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
    table_id_status = ""
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    insert_job = insert_client.query(insert_file_names_sql,
                                     location="US", job_config=job_config)
    insert_job.result()    
    
    xcom = kwargs["ti"]
    xcom.xcom_push(key="customer_id", value=customer_id)
    print("CUSTOMER_ID:", xcom.xcom_pull(key="customer_id"))

# execute big query statement
def execute_query(query):
    client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
    query_job = client.query(
        query
    )
    results = query_job.result()
    if results.total_rows == 0:
        return None
    for row in results:
        return row.file_name

# get the outer file name from ctl_aafs_customer_files table
def get_outer_file_name():
    
    query ="SELECT file_name \
from `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files` \
where outer_file_flag = 'Y' AND processed_flag = 'N' ORDER BY file_name LIMIT 1".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    
    query_result = execute_query(query)
    print("QUERY:", query_result)
    return query_result

# copy file from source to target bucket    
def copy_file(ingress_bucket_name: str, pipeline_bucket_name: str, source_files: str, destination_files: str,):
    source_bucket = storage_client_impersonated.bucket(ingress_bucket_name)
    destination_bucket = storage_client_impersonated.bucket(pipeline_bucket_name)
    for source_path, destination_path in zip(source_files, destination_files):
        print("SOURCE BUCKET:", ingress_bucket_name)
        print("DESTINATION BUCKET:", pipeline_bucket_name)
        print("SOURCE FILES:", source_path)
        source_blob = source_bucket.blob(source_path)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_path
        )

# copy zip file to staging bucket
def copy_zip_to_staging():
    #prefix  = "ads-aafs/"
    #regex = prefix + "aafs_amex_" + "w+"
    #blob_path = scan_bucket_for_file(prefix, regex)
    blob_path = get_outer_file_name()
    copy_file(INGRESS_EFG_BUCKET_NAME,PIPELINE_BUCKET_NAME, [str(blob_path)], [str(blob_path)])
    return blob_path

# unzip the file in staging bucket and load the data into control table
def unzip_file_in_staging_and_load_ctl(**kwargs):
    stage_bucket = PIPELINE_BUCKET_NAME
    bucket = storage_client_impersonated.get_bucket(stage_bucket)
    zipfilename_with_path = get_outer_file_name() #'ads-aafs/aafs_amex_optin_list_20220607_csv.zip'
    destination_blob_pathname = zipfilename_with_path
    blob = bucket.blob(destination_blob_pathname)
    zipbytes = io.BytesIO(blob.download_as_string())
    
    InnerFileNames = []
        
    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                InnerFileNames.append(zipfilename_with_path + "/" + contentfilename)
                blob = bucket.blob(zipfilename_with_path + "/" + contentfilename)
                blob.upload_from_string(contentfile)
                print("CONTENT FILE NAME:", contentfilename)
    print("INNER FILE NAMES:",InnerFileNames)
    #if InnerFileNames:
        #load_ctl_inner_customer_aafs_table(InnerFileNames
    xcom = kwargs["ti"]
    xcom.xcom_push(key="InnerFileNames", value=InnerFileNames)
    return InnerFileNames

# parent function to load the staging data into control table ctl_aafs_customer_files
def load_ctl_inner_customer_aafs_table(InnerFileNames):
    file_row_values,_ = prepare_ctl_customer_aafs(InnerFileNames,IsInner=True)
    insert_file_names_sql = f"INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files` VALUES " + ",".join(
        file_row_values)
    print(insert_file_names_sql)

    insert_client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
    table_id_status = ""
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    insert_job = insert_client.query(insert_file_names_sql,
                                     location="US", job_config=job_config)
    insert_job.result()

# task for truncating control table ctl_aafs_customer_files
bq_ctl_table_truncate = BigQueryExecuteQueryOperator(
task_id='bq_ctl_table_truncate',
destination_dataset_table="{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
sql="SELECT * FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files` where 1=0".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
bigquery_conn_id='google_cloud_default',
impersonation_chain=SERVICE_ACCOUNT,
use_legacy_sql=False,
write_disposition='WRITE_TRUNCATE',
labels=GCP_COST_LABELS_BIGQUERY,
dag=dag
)

# get inner files count + 1 from database control table ctl_aafs_customer_files
def get_inner_files_count():
    query = "select distinct file_name from {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files where outer_file_flag='N' AND processed_flag='N/A'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    #result = execute_query(query)
    client = get_bq_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
    query_job = client.query(
        query
    )
    results = query_job.result()
    return results.total_rows + 1

# load inner file names into control ctl_aafs_customer_files table
def load_csv_names_into_ctl_customer_aafs_table(*args, **kwargs):
    xComm_var = kwargs['ti']
    InnerFileNames = xComm_var.xcom_pull(task_ids='unzip_file_in_staging_and_load_ctl')
    
    if InnerFileNames:
        load_ctl_inner_customer_aafs_table(InnerFileNames)
    
    return 0

# global variable to assign the count value periodically
DynamicWorkflow_Group1 = get_inner_files_count()

# task to load the csv file names to control ctl_aafs_customer_files table
task_load_csv_names_into_ctl_customer_aafs_table = PythonOperator(
    task_id='load_csv_names_into_ctl_customer_aafs_table',
    dag=dag,
    provide_context=True,
    python_callable=load_csv_names_into_ctl_customer_aafs_table,
    op_args=[])

# prepare sads_aafs_optin table row data from inner csv file
def add_columns(counter, row:list, file_name):
    row.append(",".join(row))
    row.insert(0, counter)
    print("ROW:", row)
    row = f"({row[0]}, '{row[1]}', '{row[2]}','{row[3]}','{row[4]}','{row[5]}', '{row[6]}', '{row[7]}', '{file_name}')"

    return row

# truncate sads_aafs_optin table and insert inner file data
def clear_sads_aafs_optin_and_load_inner_csv_file(*args, **kwargs):
    #clear the table
    truncate_statement ="TRUNCATE TABLE `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_aafs_optin`".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
    query_job = bq_client.query(truncate_statement)
    
    xcom = kwargs["ti"]
    counter_idx = args[0]
    inner_file_names = xcom.xcom_pull(task_ids="unzip_file_in_staging_and_load_ctl")
    print("CURRENT_FILE_IDX:",counter_idx)
    if counter_idx < len(inner_file_names):
        print("CURRENT_FILE:",inner_file_names[int(counter_idx)])
        file_name = inner_file_names[int(counter_idx)]
        file_row = download_blob_into_memory(PIPELINE_BUCKET_NAME, file_name)
        final_data = []        
        row_counter = 1
        sql_statement = "INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_aafs_optin` \
        (line_id,region_cd,country_cd,agency_cd,vendor_type_cd,vendor_cd,action_cd,line_txt,file_name) \
        VALUES ".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
        
        file_row = [row for row in file_row]
        file_row = list(filter(None,file_row))
        print("FILE CONTENTS",file_row)
        if len(file_row) > 1:
            for row in file_row[1:]:
                final_data.append(add_columns(row_counter,row,file_name.split("/")[-1]))
                row_counter +=1
            sql_statement += ",".join(final_data)
            print("FINAL INSERT QUERY", sql_statement)
            query_job = bq_client.query(sql_statement)
            result = query_job.result()
        else:
            raise AirflowSkipException
    # errors = client.insert_rows_json(table_id, final_data)  # Make an API request.
    # if errors == []:
    #     print("New rows have been added.")
    # else:
    #     print("Encountered errors while inserting rows: {}".format(errors))            

# validating inner file type and file name 
def isFileCSV(filename):
    extension = filename.split(".")[-1]
    if extension.lower() == "csv": #6K1F_20220607.csv
        file_name = filename.split("/")[-1] 
        if ((not re.search('^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]_2[0-9][0-9][0-9][0-1][0-9][0-3][0-9]\....$', file_name)) and    
            (not re.search('^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]_2[0-9][0-9][0-9][0-1][0-9][0-3][0-9]_GDS\....$', file_name)) and   
            (not re.search('^[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]_2[0-9][0-9][0-9][0-1][0-9][0-3][0-9]_[0-9][0-9]\....$', file_name))):
            print("FILE NAME:", file_name)
            print("NAME NOT MATCHED")
            #inner_file_name_warning_email(environment=ENVIRONMENT, email_type="Warning", dag_name=DAG_NAME, file_name=filename, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP")
            return inner_file_name_warning_email
        print("EXTENSION IS CSV")
        return None
    print("FILE FORMAT NOT CORRECT")
    #inner_file_type_warning_email(environment=ENVIRONMENT, email_type="Warning", dag_name=DAG_NAME, file_name=filename, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP")
    return inner_file_type_warning_email
    
# validate header row
def checkHeaderRow(header: list, error: list):
    columns_names = [column_name.strip() for column_name in header] # strip() --trim leading and trailing spaces
    
    try:
        if len(columns_names) != 6: # length of total columns
            error.append(f"[Line 0] - Parsed {len(columns_names)} header line fields, should be 6.")
            if len(columns_names[0]) > 11 and columns_names[0][11] != ',': #checking first row and first column
                error.append(f"[Line 0] - Check delimiter, fields should be comma separated.")
    except Exception as e: # capture out of index on column_name
        print("DISOLVING ERROR",str(e))
        
    try:        
        if columns_names[0].lower() != "region_code":
            error.append("[Line 0][Field 1] - Invalid header, should be REGION_CODE.")
        if columns_names[1].lower() != "country_code":
            error.append("[Line 0][Field 2] - Invalid header, should be COUNTRY_CODE.")
        if columns_names[2].lower() != "agency_code":
            error.append("[Line 0][Field 3] - Invalid header, should be AGENCY_CODE.")
        if columns_names[3].lower() != "vendor_type":
            error.append("[Line 0][Field 4] - Invalid header, should be VENDOR_TYPE.")
        if columns_names[4].lower() != "vendor_code":
            error.append("[Line 0][Field 5] - Invalid header, should be VENDOR_CODE.")
        if columns_names[5].lower() != "action_code":
            error.append("[Line 0][Field 6] - Invalid header, should be ACTION_CODE.")
    except Exception as e:
        print("DISOLVING ERROR-2",str(e))
    return error

# download the file object into composer memory
def download_blob_into_memory(bucket_name, blob_name):
    """Downloads a blob into memory."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # blob_name = "storage-object-name"

    bucket = storage_client_impersonated.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(blob_name)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    blob = StringIO(blob)
    file_contents = csv.reader(blob, delimiter=',')
    return file_contents

## beginning of "validate" step from OptinCleanse.nawk script ##
# validate inner file's file type, file name, header and data row  
def validate_inner_file(*args, **kwargs):    
    xcom = kwargs["ti"]
    counter_idx = args[0]
    inner_file_names = xcom.xcom_pull(task_ids="unzip_file_in_staging_and_load_ctl")
    customer_id=xcom.xcom_pull(key="customer_id")
    print("CURRENT_FILE_IDX:",counter_idx)
    if inner_file_names: # check if no inner files
        if counter_idx < len(inner_file_names): # check counter_idx of dynamic task group with the count of the inner files
            print("CURRENT_FILE:",inner_file_names[int(counter_idx)])
            file_name = inner_file_names[int(counter_idx)]
            error = []        
            header_error = 0
            file_error_fun = isFileCSV(file_name)
            empty_file_flag = False
            if file_error_fun is not None: # if inner file type is not csv or file name is incorrect
                file_error_fun(environment=ENVIRONMENT, email_type="Warning", file_name=file_name, product="AAFS Opt-in List Load", customer_id=customer_id)
                raise AirflowSkipException
            else: # check header and data rows
                file_contents = download_blob_into_memory(PIPELINE_BUCKET_NAME, file_name)
                counter = 0
                file_contents = list(filter(None,file_contents))
                if len(file_contents) < 1: # empty file
                    error.append("Empty file {file_name}".format(file_name=file_name.split("/",1)[1]))
                    header_error = len(error)
                    empty_file_flag = True
                elif len(file_contents) < 2: # header but no data row
                    error = checkHeaderRow(file_contents[0], error)
                    header_error = len(error)
                    error.append("No data rows in file {file_name}".format(file_name=file_name.split("/",1)[1]))
                else:   # data rows exist   
                    for row in file_contents:
                        print("ROW:", row)
                        if counter == 0: # check header row column names
                            error = checkHeaderRow(row, error)
                            header_error = len(error)                        
                        else: # checking data rows
                            error = checkRow(row, counter, error)
                        counter += 1
                print("ERROR:",error)
                if error: # if any errors found above, send warning email to customer
                    inner_file_contents_error_email(error[:header_error], error[header_error:], file_name.split("/")[-1], customer_id,empty_file_flag)
                    raise AirflowSkipException
        else:
            raise AirflowSkipException
    else:
        raise AirflowSkipException
        
# validate row data of each cell for accepted value vs. required value
def checkCell(cell_value: str, field_no: int, accepted_len: int, regex: str, is_alpha: bool, error: list, line_num, error_detail: list):
    if is_alpha:
        if len(cell_value) == 0:
            error.append(f"[Line {line_num}][Field {field_no}] - Blank field - not allowed. Value required.")
        elif len(cell_value) != accepted_len:
            error.append(f"[Line {line_num}][Field {field_no}] {error_detail[0]} length is {len(cell_value)} {error_detail[1]}")
            
        elif not re.match(regex, cell_value):
            error.append(f"[Line {line_num}][Field {field_no}] {error_detail[0]} length is {len(cell_value)} {error_detail[1]}")
    else:
        if len(cell_value) == 0:
            error.append(f"[Line {line_num}][Field {field_no}] - Blank field - not allowed. Value required.")
        if cell_value != '**':
            if len(cell_value) != accepted_len:
                error.append(f"[Line {line_num}][Field {field_no}] {error_detail[0]} length is {len(cell_value)} {error_detail[1]}")
            elif not re.match(regex, cell_value):
                error.append(f"[Line {line_num}][Field {field_no}] {error_detail[0]} length is {len(cell_value)} {error_detail[1]}")
    return error

# validating total row with accepted data pattern
def checkRow(row: str, line_num, error: list):
    cell_values = [cell_value.strip() for cell_value in row]
    
    try:
        if len(cell_values) != 6:
            error.append(f"[Line {line_num}] - Parsed {len(cell_values)} fields, should be 6.")
            if len(cell_values[0]) > 3 and cell_values[0][3] != ',': # checking first row and first column
                error.append(f"[Line {line_num}] - Check delimiter, fields should be comma separated.")
    except Exception as e:
        print("DATA DISOLVING ERROR",str(e))
        
    try:
        error = checkCell(cell_value=cell_values[0], field_no=1, accepted_len=3, regex=r"[a-zA-z][a-zA-Z][a-zA-z]$",
                                 is_alpha=True, error=error, line_num=line_num, error_detail=['REGION_CODE','should be 3 alpha characters.'])
        error = checkCell(cell_value=cell_values[1], field_no=2, accepted_len=2, regex=r"\*\*$|[A-Za-z][A-Za-z]$",
                                 is_alpha=True, error=error, line_num=line_num, error_detail=['COUNTRY_CODE','should be ** or 2 alpha characters.'])
        error = checkCell(cell_value=cell_values[2], field_no=3, accepted_len=4, regex=r"\*\*$|[A-Za-z0-9][A-Za-z0-9][A-Za-z0-9][A-Za-z0-9]$", 
                                 is_alpha=False, error=error, line_num=line_num, error_detail=['AGENCY_CODE','should be ** or 4 alphanumeric characters.'])
        error = checkCell(cell_value=cell_values[3], field_no=4, accepted_len=3, regex=r"[A-Za-z][A-Za-z][A-Za-z]$", 
                                 is_alpha=True, error=error, line_num=line_num, error_detail=['VENDOR_TYPE','should be 3 alpha characters.'])
        error = checkCell(cell_value=cell_values[4], field_no=5, accepted_len=2, regex=r"[A-Za-z0-9][A-Za-z0-9]", 
                                 is_alpha=True, error=error, line_num=line_num, error_detail=['VENDOR_CODE','should be 2 alphanumeric characters.'])
        error = checkCell(cell_value=cell_values[5], field_no=6, accepted_len=1, regex=r"[Aa]|[Dd]", 
                                 is_alpha=True, error=error, line_num=line_num, error_detail=['ACTION_CODE','should be 1 [A or D].'])
    
    except Exception as e:
        print("DATA DISOLVING ERROR-2",str(e))
    return error

# push current_sc_pcc, current_file_type and current_file_name values into xcom global variables
def extract_sc_pcc_and_file_type_from_file_name(*args, **kwargs):
    xComm_var = kwargs['ti']
    file_index = args[0]
    innerFileNames = xComm_var.xcom_pull(task_ids="unzip_file_in_staging_and_load_ctl")
    if file_index < len(innerFileNames):        
        file_path = innerFileNames[file_index]
        '''
        if file_index != 0:
            print("sc_pcc of the file " + str(file_index) + xComm_var.xcom_pull(key="current_sc_pcc"))
            print("File type for file " + str(file_index) + xComm_var.xcom_pull(key="current_file_type"))
        innerFileNames[index] returns a value as follows:
        'ads-aafs/aafs_amex_optin_list_20220624_csv.zip/G1Y0_20220624.csv'
        therefore, we split it based on the "/" which returns ['ads-aafs', 'aafs_amex_optin_list_20220624_csv.zip', 'G1Y0_20220624.csv']
        from this returned list we take the last element('G1Y0_20220624.csv') which will be the name of the file for extracting
        the sc_pcc code and the file type. 
        '''
        file_name = file_path.split("/")[-1]
        print("Name of the file to be processed: " + file_name)
        print("File Path: " + file_path)
        sc_pcc = file_name[:4]
        # we determine the file type based on the length of the file_name
        file_name_length = len(file_name)
        if(file_name_length == 17):
            file_type = 'A'
        # todo: verify the second condition
        elif(file_name_length == 21 and file_name[14:17].upper() == 'GDS'):
            file_type = 'G'
        else:
            file_type = 'Q'
        print("sc_pcc = " + sc_pcc)
        print("file_type = " + file_type)
        xComm_var.xcom_push("current_sc_pcc", sc_pcc)
        xComm_var.xcom_push("current_file_type", file_type)
        xComm_var.xcom_push("current_file_name", file_name)

# open sql script file and prepare the SELECT statement
def get_error_count_query_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/L 13 select count wads_aafs_optin_error.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    return query

# get the error count from wads_aafs_optin_error table for errors from SQLs L 06 to L 12
def get_errors_count():
    error_count = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    error_query = get_error_count_query_from_sql()
    print("query:" + error_query)
    error_count_query_result = bq_client.query(error_query, location="US", job_config=job_config)
    if error_count_query_result.result().total_rows == 0:
        print("No results")
    elif error_count_query_result.result().total_rows == 1:
        for row in error_count_query_result.result():
            print("Error count in optin_error table = " + str(row.error_count))
            error_count = row.error_count
    return error_count

# this function will determine the course of action based on the error_count in wads_aafs_optin_error table to send warning email to customer if errors from SQLs L 06 to L 12
def redirect_if_errors_present(*args,**kwargs):
    file_index = args[0]
    error_count = get_errors_count()
    if(error_count > 0):
        return "exception_case_scenario_" + str(file_index)
    return "happy_path_scenario_" + str(file_index)

# replace query parameters to retrieve file id
def retrieve_file_id_query_from_sql(file_name):
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/L 15 max file from optin_header.sql', 'r')
    query = sql_file.read() \
        .replace("{{ params.LAKEHOUSE_PROJECT_ID }}", LAKEHOUSE_PROJECT_ID) \
        .replace("{{ aafs_file_name }}", file_name)
    sql_file.close()
    return query

# retrieve file id query from current inner file and execute the query, push current_file_id value to xcom global variable
def retrieve_file_id_for_current_inner_file_from_optin_header(*args, **kwargs):
    file_id = 0
    xComm_var = kwargs['ti']
    file_name = xComm_var.xcom_pull(key='current_file_name')
    print("FILE_NAME:",file_name)
    retrieve_file_id_query = retrieve_file_id_query_from_sql(file_name)
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("Query to retrieve file_id:" + retrieve_file_id_query)
    retrieve_file_id_query_result = bq_client.query(retrieve_file_id_query, location="US", job_config=job_config)
    if retrieve_file_id_query_result.result().total_rows == 0:
        print("No results")
    elif retrieve_file_id_query_result.result().total_rows == 1:
        for row in retrieve_file_id_query_result.result():
            file_id = row.file_id
            print("Latest file id for file {} is {}".format(file_name, str(file_id)))
    xComm_var.xcom_push("current_file_id", file_id)

def get_error_txt_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/R 17 select wads_aafs_optin_error.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    # print(query)
    return query

def get_error_txt():
    error = []
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + get_error_txt_from_sql())
    error_txt_query_result = bq_client.query(get_error_txt_from_sql(), location="US",
                                                     job_config=job_config)
    for err in error_txt_query_result:
        error.append(err.error_txt)
    print("QUERY ERROR RESULT:", error)
    return error
    
def inner_file_data_row_errors_warning_email(*args,**kwargs):
    xcom = kwargs["ti"]    
    customer_id=xcom.xcom_pull(key="customer_id")
    innerFileNames = xcom.xcom_pull(task_ids='unzip_file_in_staging_and_load_ctl')
    file_index = args[0]
    file_name = innerFileNames[file_index]
    print("file name: ", file_name)
    print("CUSTOMER_ID", customer_id)
    error_txt = get_error_txt()
    body = """ """
    body += """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """Greetings,<br /><br />"""
    body += """<strong>Warning:</strong> The AAFS opt-in list with filename %s could not be loaded.<br /><br />""" %(file_name.split("/", 1)[1])
    body += """<strong>Action:</strong> Please review the error log details below and make corrections. Then, resend the corrected opt-in list file. For more information, please contact %s. <br /><br />""" %(WARNING_RECIPIENT_EMAIL)
    body += """<strong>File Data Row Errors</strong> <br />"""
    for error in error_txt:
        body += """%s <br /> """ % (error)
    body += """<br /> """
    body += """<br>Thank you,<br />AdMedia Support<br />"""    
    email_subject = """%s %s for %s""" %(ENVIRONMENT, "Warning", "AAFS Opt-in List Load")
    
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL)  

def inner_file_success_email(*args, **kwargs):
    xComm_var = kwargs['ti']
    innerFileNames = xComm_var.xcom_pull(task_ids='unzip_file_in_staging_and_load_ctl')    
    customer_id=xComm_var.xcom_pull(key="customer_id")    
    file_index = args[0]
    file_name = innerFileNames[file_index]
    print("file name: ", file_name)
    body = """ """
    body += """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """Greetings,<br /><br />"""
    body += """<strong>Success:</strong> A new AAFS opt-in list with filename %s has been received and loaded.""" %(file_name.split("/", 1)[1])
    print( "Success: A new AAFS opt-in list with filename %s has been received and loaded." , (
            file_name.split("/", 1)[1]))
    body += """ For more information, please contact %s. <br />""" % (
            WARNING_RECIPIENT_EMAIL)
    body += """<br /> """
    body += """<br>Thank you,<br />AdMedia Support<br />"""
    email_subject = """%s %s for %s""" % (ENVIRONMENT, "Success", "AAFS Opt-in List Load")
    print("CUSTOMER_ID", customer_id)
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=WARNING_RECIPIENT_EMAIL) 


# declare dynamic empty task list
task_inner = []

# add the dynamic tasks to the list task_inner
for index in range(int(DynamicWorkflow_Group1)):    
    task_inner.extend([PythonOperator(
            task_id='validate_inner_csv_file_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            trigger_rule='none_failed',
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=validate_inner_file),
        PythonOperator(
            task_id='extract_sc_pcc_and_file_type_from_file_name_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=extract_sc_pcc_and_file_type_from_file_name),        
        PythonOperator(
            task_id='clear_sads_aafs_optin_and_load_inner_csv_file_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=clear_sads_aafs_optin_and_load_inner_csv_file),
        BigQueryExecuteQueryOperator(
            task_id='validate_more_inner_csv_file_data_' + str(index),
            sql='L 06-12 load_customer_region_insert_invalid_data_into_wads_aafs_optin_error.sql',
            use_legacy_sql=False,
            bigquery_conn_id='google_cloud_default',
            provide_context=True,
            location=BQ_DATASET_LOCATION,
            impersonation_chain=SERVICE_ACCOUNT,
            params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
            labels=GCP_COST_LABELS_BIGQUERY,
            dag=dag),
        BranchPythonOperator(
            task_id='check_if_errors_in_wads_aafs_optin_error_' + str(index),
            provide_context=True,
            trigger_rule='none_failed_or_skipped',
            python_callable=redirect_if_errors_present,
            labels=GCP_COST_LABELS_PYTHON_API,
            op_args=[index],
            dag=dag),
        dummy_operator.DummyOperator(
            task_id='exception_case_scenario_' + str(index),
            trigger_rule='none_failed_or_skipped',
            dag=dag),
        dummy_operator.DummyOperator(
            task_id='happy_path_scenario_' + str(index),
            trigger_rule='none_failed_or_skipped',
            dag=dag),
        PythonOperator(
            task_id='inner_file_data_row_errors_warning_email_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=inner_file_data_row_errors_warning_email),
        BigQueryExecuteQueryOperator(
            task_id='insert_optin_file_data_to_optin_header_' + str(index),
            sql='L 14 insert into optin_header.sql',
            use_legacy_sql=False,
            bigquery_conn_id='google_cloud_default',
            provide_context=True,
            trigger_rule='none_failed_or_skipped',
            location=BQ_DATASET_LOCATION,
            impersonation_chain=SERVICE_ACCOUNT,
            params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
            labels=GCP_COST_LABELS_BIGQUERY,
            dag=dag),
        PythonOperator(
            task_id='retrieve_file_id_for_current_file_from_optin_header_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            trigger_rule='none_failed_or_skipped',
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=retrieve_file_id_for_current_inner_file_from_optin_header),
        BigQueryExecuteQueryOperator(
            task_id='load_optin_details_optin_apply_details_and_aafs_agency_vendor_last_' + str(index),
            sql='L 16-22 load optin_details optin_apply_details and aafs_agency_vendor_last.sql',
            use_legacy_sql=False,
            bigquery_conn_id='google_cloud_default',
            provide_context=True,
            location=BQ_DATASET_LOCATION,
            impersonation_chain=SERVICE_ACCOUNT,
            params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID, "PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
            labels=GCP_COST_LABELS_BIGQUERY,
            dag=dag),
        PythonOperator(
            task_id='send_inner_file_success_email_' + str(index),
            dag=dag,
            op_args=[index],
            provide_context=True,
            labels=GCP_COST_LABELS_PYTHON_API,
            python_callable=inner_file_success_email)
            ])
    # Note: The last pass through the inner loop will always be for the dummy file processing
    if index==0: # for the initial (1st) pass through the inner loop, whether inner file exists or not, these tasks will be executed. Note: -1 index will be the last task
        task_load_csv_names_into_ctl_customer_aafs_table >> task_inner[-12] >> task_inner[-11] >> task_inner[-10] >> task_inner[-9] >> task_inner[-8] >> [task_inner[-7], task_inner[-6]]
        task_inner[-7] >> task_inner[-5]
        task_inner[-6] >> task_inner[-4] >> task_inner[-3] >> task_inner[-2] >> task_inner[-1]
    else: # on the 2nd through Nth pass through the inner loop, if an incoming file exists, this block will be executed. Note: -1 index will be the last task
        task_inner[-17] >> task_inner[-12] # TO DO: add comments for exception scenario path
        task_inner[-13] >> task_inner[-12] >> task_inner[-11] >> task_inner[-10] >> task_inner[-9] >> task_inner[-8] >> [task_inner[-7], task_inner[-6]]
        task_inner[-7] >> task_inner[-5]
        task_inner[-6] >> task_inner[-4] >> task_inner[-3] >> task_inner[-2] >> task_inner[-1]

def check_optin_error_count_for_too_many_vendors_query_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/R 16 count wads_aafs_optin_error.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    # print(query)
    return query

#to get the load_optin_error_count value from query result of sql
def check_optin_error_count_for_too_many_vendors():
    optin_error_count = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + check_optin_error_count_for_too_many_vendors_query_from_sql())
    optin_error_count_query_result = bq_client.query(check_optin_error_count_for_too_many_vendors_query_from_sql(), location="US",
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

def send_warning_email_for_too_many_vendors(*args, **kwargs):    
    xcom = kwargs["ti"]    
    customer_id=xcom.xcom_pull(key="customer_id")
    print("CUSTOMER_ID", customer_id)
    error_txt = get_error_txt()
    body = """ """
    body += """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """Greetings,<br /><br />"""
    body += """<strong>Warning:</strong> The AAFS vendor count threshold of %s has been exceeded.<br /><br />""" %(MAX_PCC_VENDORS)
    body += """<strong>PCCs for which too many vendors have been selected:</strong>"""
    body += """<ul>"""
    for error in error_txt:
        body += """<li> %s </li>""" % (error)
    body += """</ul>"""
    body += """<strong>Action:</strong> Please review the PCCs above; then, send an opt-in list file that will reduce the number of vendors for each PCC above. For more information, please contact %s. <br /><br />""" %(WARNING_RECIPIENT_EMAIL)
        
    body += """<br>Thank you,<br />AdMedia Support<br />"""    
    email_subject = """%s %s for %s""" %(ENVIRONMENT, "Warning", "AAFS Opt-in List Load")
    
    if customer_id==1:
        send_warning_email(recipient_email=AMEX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL)
    #else: # for future customers recipient_email will be XXXX_RECIPIENT_EMAIL
        #send_warning_email(recipient_email=XXXX_RECIPIENT_EMAIL, subject=email_subject, body=body, cc=WARNING_RECIPIENT_EMAIL) 

def check_optin_error_count():
    optin_error_count=check_optin_error_count_for_too_many_vendors()
    if (optin_error_count == 0):
        print("No error txt")
    elif (optin_error_count>0):
        return "send_aafs_refresh_warning_mail_for_too_many_vendors"
    return "check_optin_header_target_table_updates_after_completing_all_inner_csv_file_processing"

send_aafs_refresh_warning_mail_for_too_many_vendors = PythonOperator(
    task_id='send_aafs_refresh_warning_mail_for_too_many_vendors',
    #trigger_rule='none_failed',
    python_callable=send_warning_email_for_too_many_vendors,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
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
    trigger_rule="none_failed_or_skipped",
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

def get_optin_header_updates_from_sql():
    sql_file = open(AAFS_PARAMS_PATH + '/ads-aafs/aafs/E1 select count optin header.sql', 'r')
    query = sql_file.read().replace("{{ params.LAKEHOUSE_PROJECT_ID }}", LAKEHOUSE_PROJECT_ID)
    sql_file.close()
    return query

def check_optin_header_table_to_see_if_need_to_update_extract_table():
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    print("query:" + get_optin_header_updates_from_sql())
    optin_header_updates_query_result = bq_client.query(get_optin_header_updates_from_sql(), location="US", job_config=job_config)
    print("query_result.result().total_rows", optin_header_updates_query_result.result().total_rows)
    for row in optin_header_updates_query_result.result():
        if int(row.Nbr_Of_Additional_Rows) == 0:
            print("NO OF ADDITIONAL ROWS-IF",row.Nbr_Of_Additional_Rows)
            return "skip_E2_E3_E4_SQLs"
        elif int(row.Nbr_Of_Additional_Rows) > 0:
            print("NO OF ADDITIONAL ROWS-EIF",row.Nbr_Of_Additional_Rows)
            return "prepare_extract_data_from_aafs_agency_vendor_last"

def check_update_extract_table():
    if (UPDATE_EXTRACT_TABLE == "Y"):
        return "place_final_extract_data_into_aafs_extract_out"
    return "archive_delete_outer_files"

# archive the source files to archive bucket and delete the files from source and staging buckets        
def archive_delete_outer_files():
    file_name = get_outer_file_name()
    source_bucket = storage_client_impersonated.bucket(INGRESS_EFG_BUCKET_NAME)
    destination_bucket = storage_client_impersonated.bucket(ARCHIVE_BUCKET_NAME)
    print("SOURCE BUCKET:", INGRESS_EFG_BUCKET_NAME)
    print("DESTINATION BUCKET:", ARCHIVE_BUCKET_NAME)
    print("SOURCE FILE:", file_name)
        
    source_blob = source_bucket.blob(file_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, file_name
    )
    source_bucket.delete_blob(file_name)
    source_bucket.delete_blob(file_name + '.done')
    stage_bucket = storage_client_impersonated.bucket(PIPELINE_BUCKET_NAME)
    blobs = storage_client_impersonated.list_blobs(PIPELINE_BUCKET_NAME,prefix='ads-aafs/')
    for blob in blobs:
        stage_bucket.delete_blob(str(blob.name))

# update the process_flag in the control table for the incoming outer file, and check for any remaining unprocessed outer files
def update_and_check_ctl_outer_file_flag():
    file_name = get_outer_file_name()
    sql_update = "UPDATE `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_aafs_customer_files` \
    SET processed_flag = 'Y' \
    WHERE outer_file_flag = 'Y' AND file_name = '{file_name}' ".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID,file_name=file_name)
    
    query_result = execute_query(sql_update)
    print("SQL_UPDATE:",sql_update)
    next_outer_file_name = get_outer_file_name()
    print("NEXT OUTER FILE NAME:",next_outer_file_name)
    if next_outer_file_name is None:
        outer_success_email(environment=ENVIRONMENT, email_type="Success", dag_name=DAG_NAME, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP")
        return "stop_dag_run_op"
    else:
        outer_success_email_with_restart(environment=ENVIRONMENT, email_type="Success", dag_name=DAG_NAME, product="ads-aafs", assets_tag="ADS:AAFS-LOAD-GCP")
        return "trigger_dag_run"

start = dummy_operator.DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    dag=dag
)

task_DummyForward = dummy_operator.DummyOperator(
    task_id='DummyForward',
    trigger_rule='all_success',
    dag=dag
)

task_check_outer_customer_zip_file = BranchPythonOperator(
    task_id='check_outer_customer_zip_file',
    python_callable=check_outer_customer_zip_file,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_load_zip_into_ctl_customer_aafs_table= PythonOperator(
    task_id='load_zip_into_ctl_customer_aafs_table',
    python_callable=load_zip_into_ctl_customer_aafs_table,
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_skip_dag_run= PythonOperator(
    task_id='skip_dag_run',
    python_callable=skip_dag_run,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)
    
task_copy_zip_to_staging= PythonOperator(
    task_id='copy_zip_to_staging',
    python_callable=copy_zip_to_staging,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_unzip_file_in_staging_and_load_ctl= PythonOperator(
    task_id='unzip_file_in_staging_and_load_ctl',
    python_callable=unzip_file_in_staging_and_load_ctl,
    labels=GCP_COST_LABELS_PYTHON_API,
    provide_context=True,
    dag=dag
)

check_optin_header_target_table_updates_after_completing_all_inner_csv_file_processing = BranchPythonOperator(
    task_id='check_optin_header_target_table_updates_after_completing_all_inner_csv_file_processing',
    python_callable=check_optin_header_table_to_see_if_need_to_update_extract_table,
    labels=GCP_COST_LABELS_PYTHON_API,
    #trigger_rule='none_failed',
    dag=dag
)

task_skip_E2_E3_E4_SQLs = dummy_operator.DummyOperator(
    task_id='skip_E2_E3_E4_SQLs',
    trigger_rule='all_success',
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

task_check_update_extract_table = BranchPythonOperator(
    task_id='check_update_extract_table',
    python_callable=check_update_extract_table,
    trigger_rule="all_success",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_archive_delete_outer_files = PythonOperator(
    task_id='archive_delete_outer_files',
    python_callable=archive_delete_outer_files,
    trigger_rule="none_failed_or_skipped",
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_trigger_dag = BranchPythonOperator(
    task_id='update_and_check_ctl_outer_file_flag',
    python_callable=update_and_check_ctl_outer_file_flag,
    #trigger_rule="none_failed",
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

# check if an outer file exists in efg ingress bucket
start >> bq_ctl_table_truncate >> task_check_outer_customer_zip_file >> [task_skip_dag_run, task_DummyForward]

# record outer and inner file names into control table ctl_aafs_customer_files
task_DummyForward >> task_load_zip_into_ctl_customer_aafs_table >> task_copy_zip_to_staging >> task_unzip_file_in_staging_and_load_ctl >> task_load_csv_names_into_ctl_customer_aafs_table

# execute the dynamic inner file tasks defined in the task_inner[] list, and process one inner file at a time in the for loop

# After completion of all inner file processing in the dynamic for loop, depending on whether taking the happy path or exception path scenario, 
#  take one of the following two branches

# exception path scenario, from 5th step back in the loop:  archival task and check if any other outer file needs to be processed
#  Note: index -1 will be last task in for loop, index -5 will be 5th task from last in for loop,
#task_inner[-5] >> task_archive_delete_outer_files
task_inner[-5] >> count_optin_vendors_per_pcc

# happy path scenario, from last step in the loop: archival task and check if any other outer file needs to be processed
task_inner[-1] >> count_optin_vendors_per_pcc >> log_vendor_threshold_limit_errors >> check_for_vendor_count_threshold_errors 
task_load_csv_names_into_ctl_customer_aafs_table >> count_optin_vendors_per_pcc
check_for_vendor_count_threshold_errors >> [send_aafs_refresh_warning_mail_for_too_many_vendors,check_optin_header_target_table_updates_after_completing_all_inner_csv_file_processing]
check_optin_header_target_table_updates_after_completing_all_inner_csv_file_processing >> [prepare_extract_data_from_aafs_agency_vendor_last, task_skip_E2_E3_E4_SQLs] 
task_skip_E2_E3_E4_SQLs >> task_archive_delete_outer_files >> task_trigger_dag >> [trigger_dag_run_again, stop_dag_runs] >> end
send_aafs_refresh_warning_mail_for_too_many_vendors >> task_archive_delete_outer_files
task_skip_dag_run >> end
prepare_extract_data_from_aafs_agency_vendor_last >> transform_extract_data_into_wads_aafs_extract_out >> task_check_update_extract_table >> [place_final_extract_data_into_aafs_extract_out, task_archive_delete_outer_files]
place_final_extract_data_into_aafs_extract_out >> task_archive_delete_outer_files
task_load_csv_names_into_ctl_customer_aafs_table >> task_archive_delete_outer_files
