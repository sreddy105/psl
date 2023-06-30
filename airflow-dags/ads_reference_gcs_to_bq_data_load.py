# Import library
import io
import os
import tarfile
from datetime import timedelta, datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from pathlib import Path
import csv
import shutil
import pendulum
from dateutil import parser

# Config variables
ENVIRONMENT = os.getenv("env").upper()
REFERENCE_PARAMS_PATH = "/home/airflow/gcs/dags/ads-reference"  # os.environ.get('AIRFLOW_HOME')

DAG_NAME = "ads_reference_gcs_to_bq_data_load"

load_ts = datetime.now()

target_scopes = ['https://www.googleapis.com/auth/cloud-platform']

LOCAL_TZ = pendulum.timezone("America/Chicago")

#Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        params = yaml.full_load(file)
        my_config_dict = params[env]
        print(my_config_dict)
        return my_config_dict

# read values from yaml and set vars
my_config_values = read_parameters_from_file(REFERENCE_PARAMS_PATH + '/reference/config_reference.yaml',ENVIRONMENT)
PIPELINE_PROJECT_ID = my_config_values['pipeline_project_id']
INGRESS_PROJECT_ID = my_config_values['ingress_project_id']
PIPELINE_DATASET_ID = my_config_values['pipeline_dataset_id']
WARNING_RECIPIENT_EMAIL = my_config_values['warning_recipient_email']
FAILURE_RECIPIENT_EMAIL = my_config_values['failure_recipient_email']
SUCCESS_RECIPIENT_EMAIL = my_config_values['success_recipient_email']
START_DATE = my_config_values['start_date']
SERVICE_ACCOUNT = my_config_values['service_account']
SCHEDULE_INTERVAL = my_config_values['schedule_interval']
BQ_DATASET_LOCATION = my_config_values['bq_dataset_location']
LAKEHOUSE_PROJECT_ID = my_config_values['lakehouse_project_id']
LAKEHOUSE_DATASET_ID = my_config_values['lakehouse_dataset_id']
#INGRESS_BUCKET_NAME = my_config_values['ingress_bucket_name']
#INGRESS_EFG_BUCKET_NAME = my_config_values['ingress_efg_bucket_name']
INGRESS_ADMEDIA_BUCKET_NAME = my_config_values['ingress_admedia_bucket_name']
PIPELINE_BUCKET_NAME = my_config_values['pipeline_bucket_name']
ARCHIVE_BUCKET_NAME = my_config_values['archive_bucket_name']
RETRY_MINUTES_FOR_FILE_SCAN = my_config_values['retry_minutes_for_file_scan']
BASE_DAG_URL = my_config_values['base_dag_url']
BASE_BUCKET_URL = my_config_values['base_bucket_url']
REFERENCE_BUCKET_FOLDER_NAME = my_config_values['reference_bucket_folder_name']

# Default Argument
default_args = {
    'owner': 'ADS_ADMEDIA',
    'depends_on_past': False,
    'start_date': datetime(parser.parse(START_DATE).year, parser.parse(START_DATE).month, parser.parse(START_DATE).day, tzinfo=LOCAL_TZ), # '2022-01-12',
    'email': FAILURE_RECIPIENT_EMAIL,  # FAILURE_RECIPIENT_EMAIL,  ['name.name@sabre.com','name.name@sabre.com']
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5)
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
    description='ADS AdMedia Reporting-Reference Load GCP job',  # Technical service name.
    catchup=False,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,  # SCHEDULE_INTERVAL, None #The schedule times in the config file are in Central timezone. This is needed for PCC imprsns DAG sensor check on this DAG.
    max_active_runs=1,
    template_searchpath=REFERENCE_PARAMS_PATH + '/reference')

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


# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT, LAKEHOUSE_PROJECT_ID)

# Storage client with impersonated service client (between the projects)
storage_client_impersonated_pipeline = get_storage_impersonated_client(SERVICE_ACCOUNT, PIPELINE_PROJECT_ID)
storage_client_impersonated_ingress = get_storage_impersonated_client(SERVICE_ACCOUNT, INGRESS_PROJECT_ID)
storage_client_impersonated_lakehouse = get_storage_impersonated_client(SERVICE_ACCOUNT, LAKEHOUSE_PROJECT_ID)


# send warning/failure email function
def send_email(recipient_email, subject, body, attachment_location=None, cc=None):
    from airflow.utils.email import send_email_smtp
    send_email_smtp(to=recipient_email, subject=subject, html_content=body, files=attachment_location, mime_charset='utf-8', cc=cc)


# ** Generic failure email definition included here.
def send_failure_email(message, cc=None):
    email_subject = """%s Failure for ads-reference ADS:REFERENCE-LOAD-GCP %s""" % (ENVIRONMENT, DAG_NAME)
    email_body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    email_body += message
    send_email(recipient_email=WARNING_RECIPIENT_EMAIL, subject=email_subject, body=email_body, cc=cc)

def trigger_dag_success_email():
    subject = """%s Success for ads-reference ADS:REFERENCE-LOAD-GCP %s""" %(ENVIRONMENT, DAG_NAME)
    
    body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """<strong>FYI Only: No action is required.</strong> </br></br>"""
    
    body += """This AdMedia DAG, %s, has auto-detected source files for more than one day to be processed today. </br></br>""" %(DAG_NAME)
    
    body += """Due to receiving files for more than one day, the DAG needs to rerun from the beginning to process another day’s files. 
    To fully automate this rerun and avoid manual intervention, the last step of this DAG will trigger another instance of this same 
    DAG to start <u><i>after</i></u> this DAG has finished; the new instance will process another set of incoming Reference Load files.</br></br>"""  
    
    
    body += """<strong>Note</strong>: In this job’s DAG description section, the “max_active_runs” option is set to 1 to enforce that only one instance of the DAG will run at a time.  </br></br>"""    

    send_email(SUCCESS_RECIPIENT_EMAIL, subject, body)


def print_process_message():
    print("Performing dag step. ")


def print_purging_message():
    print(
        "Performing deletion of data (37 months older discontinue_ts) from target tables : site, categories, searchterm_targeting_vars")
    print("Other BQ table data deletion is taken care by the life cycle policy as they are partitioned by date!! ")

def execute_query(client, query):
    print("QUERY:",query)
    query_job = client.query(query)
    results = query_job.result()
    print("EXECUTE_QUERY_RESULTS:",results) 
    return results

def scan_bucket_and_load_file_names_table():
    rows_to_insert = []
    prefix = "ads-reference/"
    blobs = storage_client_impersonated_pipeline.list_blobs(INGRESS_ADMEDIA_BUCKET_NAME, prefix=prefix, delimiter="/")
    blobs_list = [str(blob.name) for blob in blobs]
    values_fmt_str = "('{reference_file_name}', '{file_name_date}', {file_name_ts_integer}, '{duplicate_reference_ind}', '{processed_flag}', '{load_ts}')"
    print("Files LIst:", blobs_list)
    for file_path in blobs_list:
        if file_path.split("/", 1)[-1]:
            
            reference_file_name = file_path #.split("/", 1)[-1]
            print("FILE_NAME:",reference_file_name)
            # if reference_file_name.endswith('tar.gz'):
            parsed_date_stamp = datetime.strptime(file_path.split("_")[2][:8], "%Y%m%d")
            file_name_date = parsed_date_stamp.date()
            file_name_ts_integer = file_path.split("_")[2]
            duplicate_reference_ind = "N"
            processed_flag = "N"
            rows_to_insert.append(values_fmt_str.format(reference_file_name=reference_file_name,
                                                        file_name_date=datetime.strftime(file_name_date,'%Y-%m-%d'),
                                                        file_name_ts_integer=file_name_ts_integer,
                                                        duplicate_reference_ind=duplicate_reference_ind,
                                                        processed_flag=processed_flag,
                                                        load_ts=datetime.strftime(load_ts,'%Y-%m-%d %H:%M:%S')))

    return rows_to_insert
    
def load_reference_file_names_table():
    rows = scan_bucket_and_load_file_names_table()
    print("RECORDS:",rows)
    if rows:
        insert_sql_query = "INSERT INTO `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` VALUES ".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
        insert_sql_query += ",".join(rows)
        execute_query(bq_client, insert_sql_query)
    else:
        scan_ingress_bucket_for_reference_files()

duplicate_files_list = []

def archive_duplicate_reference_files():
    query = "SELECT reference_file_name FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` " \
            "WHERE processed_flag='N' AND duplicate_reference_ind='Y'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
        
    results = execute_query(bq_client, query)        
    for row in results:
        duplicate_files_list.append("{}".format(row.reference_file_name))
    print("DUPLICATE_FILES",duplicate_files_list)
    if duplicate_files_list:
        archive_reference_files(duplicate_files_list)

def decide_duplicate_reference_files_archive():
    query = "SELECT count(*) as records_count FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` " \
            "WHERE processed_flag='N' AND duplicate_reference_ind='Y'".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
        
    results = execute_query(bq_client, query)
    for result in results:
        if result.records_count == 0:
            return "do_not_archive_reference_files"
        else:
            return "archive_duplicate_reference_files"

def copy_file_from_ingress_to_stage(ingress_bucket_name_vbl: str, stage_bucket_name: str, source_files: str,
                                    destination_files: str, ):
    source_bucket = storage_client_impersonated_pipeline.bucket(ingress_bucket_name_vbl)
    destination_bucket = storage_client_impersonated_pipeline.bucket(stage_bucket_name)
    for source_path, destination_path in zip(source_files, destination_files):
        source_blob = source_bucket.blob(source_path)
        blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_path)
        print("File {} in bucket {} copied to file {} in bucket {}."
              .format(source_blob.name, source_bucket.name, blob_copy.name, destination_bucket.name))


def copy_reference_files_from_admedia_ingress_bucket_to_stage_bucket(**kwargs):
    xcom_var = kwargs["ti"]
    reference_file_name = xcom_var.xcom_pull(key='scanned_ingress_reference_data_file')
    # reference_filename_timestamp_latest = get_latest_reference_file(reference_file_names)

    copy_file_from_ingress_to_stage(INGRESS_ADMEDIA_BUCKET_NAME, PIPELINE_BUCKET_NAME,
                                    [str(reference_file_name[0])],
                                    [str(reference_file_name[0])])


# def get_latest_reference_file(reference_file_names):
#     if len(reference_file_names) > 1:
#         print("Attention!! Found multiple reference files :", reference_file_names)
#         reference_file_names.sort()  # list.sort() sorts the elements in the ascending order by default
#         print("Considering only the latest reference file :",
#               reference_file_names[-1])  # lis[-1] gives us the last element in the list
#     return reference_file_names[-1]  # lis[-1] gives us the last element in the list


def scan_ingress_bucket_for_reference_files(**kwargs):
    # prefix, regex = "ads-reference", "tag"
    # blobs = storage_client_impersonated_pipeline.list_blobs(INGRESS_ADMEDIA_BUCKET_NAME, prefix=prefix)
    scanned_ingress_reference_files = []
    query = "SELECT reference_file_name FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` " \
            "WHERE file_name_ts_integer IN (SELECT MIN(file_name_ts_integer) " \
            "FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` CRFI " \
                "WHERE CRFI.duplicate_reference_ind='N' AND CRFI.processed_flag='N')".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)

    results = execute_query(bq_client, query)
    for row in results:
        scanned_ingress_reference_files.append("{}".format(row.reference_file_name))    
    print("scanned_ingress_reference_files:",scanned_ingress_reference_files)
    # file_names_list = [blob.name for blob in blobs]
    # print("file_names_list before reverse sort: {}".format(file_names_list))    
    # file_names_list.sort(reverse=True)
    # print("file_names_list after reverse sort: {}".format(file_names_list))
    scanned_ingress_reference_data_file = []
    for file_name in scanned_ingress_reference_files:
        if file_name.endswith('tar.gz'):
            if file_name + ".tag" in scanned_ingress_reference_files:
                scanned_ingress_reference_data_file.append(file_name)
                break
            else:
                email_body = """<strong>Failure:</strong> No *.tar.gz.tag file is present in the AdMedia ingress bucket %s/ads-reference to 
                                           accompany the corresponding data (*.tar.gz) file.</br> """ % (INGRESS_ADMEDIA_BUCKET_NAME)
                email_body += """Expected files are:</br>"""
                email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz</br>"""% (ENVIRONMENT)
                email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz.tag</br>"""% (ENVIRONMENT)                
                email_body += """...where <i>yyyymmdd</i> is today’s date. </br>"""
                email_body += """</br><strong>Action: AdServer On-call team, </strong>"""
                email_body += "Please correct source files and deliver to AdMedia ingress bucket. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG." % (DAG_NAME)
                email_body += """</br></br>Job will retry four times from the time of initial failure."""
                email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
                email_body += """<ul>"""
                email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
                email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
                email_body += """</ul>"""
                send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
                raise Exception('Reference tag file is missing')
        elif file_name.endswith('tar.gz.tag'):
            if file_name[:-4] in scanned_ingress_reference_files:
                scanned_ingress_reference_data_file.append(file_name[:-4])
                break
            else:
                email_body = """<strong>Failure:</strong> No data (*.tar.gz) file is present in the AdMedia ingress bucket %s/ads-reference to 
                                                   accompany the corresponding *tar.gz.tag file.</br> """ % (
                    INGRESS_ADMEDIA_BUCKET_NAME)
                email_body += """Expected files are:</br>"""
                email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz</br>"""% (ENVIRONMENT)
                email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz.tag</br>"""% (ENVIRONMENT)                
                email_body += """...where <i>yyyymmdd</i> is today’s date. </br>"""
                email_body += """</br><strong>Action: AdServer On-call team, </strong>"""
                email_body += "Please correct source files and deliver to AdMedia ingress bucket. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG." % (DAG_NAME)
                email_body += """</br></br>Job will retry four times from the time of initial failure."""
                email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
                email_body += """<ul>"""
                email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
                email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
                email_body += """</ul>"""
                send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
                raise Exception('Reference tar file is missing')

    if (len(scanned_ingress_reference_data_file) == 0):
        email_body = """<strong>Failure:</strong> No reference files found in the AdMedia ingress bucket %s/ads-reference to process.</br>""" % (
            INGRESS_ADMEDIA_BUCKET_NAME)
        email_body += """Expected files are:</br>"""
        email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz</br>"""% (ENVIRONMENT)
        email_body += """ADSERVER_%s_<i>yyyymmddhhmiss</i>_REFERENCE.tar.gz.tag</br>"""% (ENVIRONMENT)        
        email_body += """...where <i>yyyymmdd</i> is today’s date. </br>"""
        email_body += """</br><strong>Action: AdServer On-call team, </strong>"""
        email_body += "Please correct source files and deliver to AdMedia ingress bucket. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG." % (DAG_NAME)
        email_body += """</br></br>Job will retry four times from the time of initial failure."""
        email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
        email_body += """<ul>"""
        email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
        email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
        email_body += """</ul>"""
        send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
        raise Exception('No reference files to process')

    print("scanned_ingress_reference_data_file:",scanned_ingress_reference_data_file)
    xcom_var = kwargs["ti"]
    xcom_var.xcom_push(key="scanned_ingress_reference_files", value=scanned_ingress_reference_files)
    xcom_var.xcom_push(key="scanned_ingress_reference_data_file", value=scanned_ingress_reference_data_file)

''' 
    input_param: file content as string
    returns: updated file_content
    aim of the function: This function updates the timestamp data in AllCategories.txt 
        to conform to "dd-mm-yy hh:mm:ss" instead of "dd-mm-yy hh.mm.ss"
    process:
        split the entire file content based on "\n" character
        split each line based on the delimiter("^")
        replace "." to ":" for each line in the last two elements of the list obtained
        combine the list into a single line, joined by the delimiter
        combine all the lines together, joined by the "\n" character
        return the updated content
'''


def update_time_stamp_in_all_categories_file(file_content):
    line_separator = "\n"
    delimiter = "^"
    lines = file_content.split(line_separator)
    updated_lines = []
    for line in lines:
        if line != "":
            attributes = line.split(delimiter)
            attributes[-2] = attributes[-2].replace(".", ":")
            attributes[-1] = attributes[-1].replace(".", ":")
            updated_line = delimiter.join(attributes)
            updated_lines.append(updated_line)
        else:
            updated_lines.append(line)
    final_content = line_separator.join(updated_lines)
    return final_content


'''
    aim of the function: extract the tar present in the "ads-reference" folder of the stage bucket 
        extracted contents must be placed in "extracted_data" folder of the stage bucket
    
    process:
        while untarring the file, we:
            1) extract the content of each file from the tar
            2) decode the content as a string:
                a) extracted content is bytes by default, so must convert that to string
                b) must replace all "\r\n" to "\n"
                c) must replace the delimiter in the file from "|^" to "^"
            3) upload the decoded content into the bucket under the corresponding file name
'''


def extract_ref_files_in_stage_bucket(**kwargs):
    xcom_var = kwargs["ti"]
    reference_folder_contents = list(
        storage_client_impersonated_pipeline.list_blobs(PIPELINE_BUCKET_NAME, prefix="ads-reference"))
    tar_file_name = ''
    tar_file_index = 0
    for index in range(len(reference_folder_contents)):
        file = reference_folder_contents[index]
        file_name = str(file.name)
        if file_name.endswith("tar.gz"):
            tar_file_name = file_name
            tar_file_index = index
            break
    file_time_stamp = tar_file_name.split('.')[0].split('_')[-2]
    xcom_var.xcom_push(key="file_time_stamp", value=file_time_stamp)
    blob = reference_folder_contents[tar_file_index]
    file_obj = io.BytesIO(blob.download_as_string())
    print("extracting {}".format(tar_file_name))
    tar_file = tarfile.open(fileobj=file_obj)
    file_names_list = tar_file.getnames()
    bucket = storage_client_impersonated_pipeline.get_bucket(PIPELINE_BUCKET_NAME)
    folder_name = "extracted_data"
    for file_name in file_names_list:
        file_in_tar = tar_file.extractfile(file_name)
        if file_in_tar is not None:
            upload_blob = bucket.blob(folder_name + "/" + file_name)
            content = file_in_tar.read().decode("utf-8").replace("\r\n", "\n").replace("|^", "^")
            if file_name.endswith("AllCategories.txt"):
                content = update_time_stamp_in_all_categories_file(content)
            upload_blob.upload_from_string(content)
            print("successfully extracted {} and placed in bucket {}, folder {}"
                  .format(tar_file_name, PIPELINE_BUCKET_NAME, folder_name))
        else:
            print("unable to extract file {}".format(file_name))


def validate_extracted_files():
    folder_name = "extracted_data"
    print("validating contents of folder:{} in bucket:{}".format(folder_name, PIPELINE_BUCKET_NAME))
    extracted_folder_contents = list(
        storage_client_impersonated_pipeline.list_blobs(PIPELINE_BUCKET_NAME, prefix=folder_name))

    '''
    mandatory_files is the list of files that must be present in every reference.tar.gz
    these 10 files must also contain corresponding .aud files that contain the count of rows in the .txt files
    '''
    mandatory_files = ['Advertiser.txt', 'AllCategories.txt', 'Campaign.txt', 'CampaignSite.txt',
                       'CampaignTargeting.txt', 'Creative.txt', 'CreativePosition.txt', 'InsertionOrder.txt',
                       'SearchtermTargetingVars.txt', 'Site.txt']
    print("Validating that every file has a corresponding .aud file.")
    file_names = [blob.name for blob in extracted_folder_contents]
    print("file names list")
    print(file_names)
    txt_files = []
    aud_files = []
    for file_name in file_names:
        file_name = file_name.split("/")[-1]
        if file_name.endswith(".txt"):
            txt_files.append(file_name)
        elif file_name.endswith(".aud"):
            aud_files.append(file_name)

    ''' checking if all the mandatory files are present in the extracted folder location '''
    for file in mandatory_files:
        if file not in txt_files:
            email_body = "<b>Failure:</b> {} file missing in the sab-{}-dap-ads-reference-reference_stg/extracted_data folder.<br>".format(file,ENVIRONMENT.lower())
            email_body += "<br><b>Action: AdServer On-call team, </b>"
            email_body += """Please correct source files and deliver to AdMedia ingress bucket %s/ads-reference. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG.""" % (INGRESS_ADMEDIA_BUCKET_NAME,DAG_NAME)
            email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
            email_body += """<ul>"""
            email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
            email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
            email_body += """</ul>"""
            send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
            raise AirflowSkipException('{} file is missing in extracted folder {} '.format(file, folder_name))

    ''' validating that each .txt file has a corresponding .aud file '''
    for file in txt_files:
        if file + ".aud" not in aud_files:
            email_body = "<b>Failure:</b> Audit (*.aud) file is not present for file {} in the sab-{}-dap-ads-reference-reference_stg/extracted_data folder.<br>".format(file,ENVIRONMENT.lower())
            email_body += "<br><b>Action: AdServer On-call team, </b>"
            email_body += """Please correct source files and deliver to AdMedia ingress bucket %s/ads-reference. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG.""" % (INGRESS_ADMEDIA_BUCKET_NAME,DAG_NAME)
            email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
            email_body += """<ul>"""
            email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
            email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
            email_body += """</ul>"""
            send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
            raise AirflowSkipException('{} file is not having an aud file'.format(file))

    print("Successfully Validated. All files have corresponding .aud files.")

    ''' validating that the row count of the txt file matches the count in the corresponding .aud file '''
    print("Validating if the row counts in the files match that of the aud files.")
    file_name_count_map = {}
    for file in extracted_folder_contents:
        print("file name: " + file.name)
        if file.name.endswith(".txt"):
            file_row_count = file.download_as_string().decode("utf-8").count("\n") - 1
            print("line count without headers: {}".format(str(file_row_count)))
            file_name_count_map[file.name] = file_row_count
        elif file.name.endswith(".aud"):
            row_count = file.download_as_string().decode("utf-8")
            print("line count in aud: {}".format(str(row_count)))
            file_name_count_map[file.name] = int(row_count)

    aud_count_mismatch = []

    for file_name_key in file_name_count_map:
        if file_name_key.endswith(".txt"):
            if file_name_key + ".aud" in file_name_count_map:
                file_row_count = file_name_count_map[file_name_key]
                file_aud_content = file_name_count_map[file_name_key + ".aud"]
                if file_row_count != file_aud_content:
                    print("Number of lines in {}, ({}) does not match with count ({}) in {}"
                          .format(file_name_key, file_row_count, file_aud_content,
                                  file_name_key + ".aud"))
                    aud_count_mismatch.append(
                        "{} has {} rows; audit count is {}.".format(file_name_key, file_row_count,
                                                                    file_aud_content))
            else:
                email_body = "<b>Failure:</b> Audit (*.aud) file is not present for file {} in the sab-{}-dap-ads-reference-reference_stg/extracted_data folder.<br>".format(file,ENVIRONMENT.lower())
                email_body += "<br><b>Action: AdServer On-call team, </b>"
                email_body += """Please correct source files and deliver to AdMedia ingress bucket %s/ads-reference. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG.""" % (INGRESS_ADMEDIA_BUCKET_NAME,DAG_NAME)
                email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
                email_body += """<ul>"""
                email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
                email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
                email_body += """</ul>"""
                send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
                raise AirflowSkipException('aud file missing for file {} '.format(file_name_key))

    if (len(aud_count_mismatch) != 0):
        email_body = "<b>Failure:</b> The number of rows in the *.txt data file does not match the number in the *.aud audit file in the sab-{}-dap-ads-reference-reference_stg/extracted_data folder.<br>".format(ENVIRONMENT.lower())
        email_body += "<br><b>The following data files do not match their corresponding audit file count:</b><br>"
        email_body += '<br>'.join(aud_count_mismatch)
        email_body += "<br><br><b>Action: AdServer On-call team, </b>"
        email_body += """Please correct source files and deliver to AdMedia ingress bucket %s/ads-reference. Contact DNAGCPPlatformSupport@sabre.com to clear failed DAG task and restart the %s DAG.""" % (INGRESS_ADMEDIA_BUCKET_NAME,DAG_NAME)
        email_body += """</br></br><strong>D&A SRE Team Access Links:</strong>"""
        email_body += """<ul>"""
        email_body += """<li> Reference Load %s AdMedia ingress bucket: %s%s/%s </li>""" % (ENVIRONMENT,BASE_BUCKET_URL,INGRESS_ADMEDIA_BUCKET_NAME,REFERENCE_BUCKET_FOLDER_NAME)
        email_body += """<li> Reference Load %s Airflow DAG: %s%s </li>""" % (ENVIRONMENT,BASE_DAG_URL,DAG_NAME)
        email_body += """</ul>"""
        send_failure_email(email_body, cc=FAILURE_RECIPIENT_EMAIL)
        raise AirflowSkipException('file validation failed: row count mismatch with the aud file')

    print("Successfully validated. The row counts of the files match that of the aud files.")


def insert_advertiser_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_advertiser"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/Advertiser.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_advertiser table.".format(destination_table.num_rows))


def insert_categories_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_categories"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/AllCategories.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_categories table.".format(destination_table.num_rows))


def insert_campaign_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_campaign"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/Campaign.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_campaign table.".format(destination_table.num_rows))


def insert_campaign_site_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_campaign_site"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/CampaignSite.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_campaign_site table.".format(destination_table.num_rows))


def insert_campaign_targeting_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_campaign_targeting"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/CampaignTargeting.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_campaign_targeting table.".format(destination_table.num_rows))


def insert_creative_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_creative"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/Creative.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_creative table.".format(destination_table.num_rows))


def insert_creative_position_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_creative_position"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/CreativePosition.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_creative_position table.".format(destination_table.num_rows))


def insert_insertion_order_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_insertion_order"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/InsertionOrder.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_insertion_order table.".format(destination_table.num_rows))


def insert_searchterm_targeting_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_searchterm_targeting_vars"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/SearchtermTargetingVars.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_searchterm_targeting_vars table.".format(destination_table.num_rows))


def insert_site_data_into_bigquery():
    table_id = f"{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.sads_site"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        # The source format defaults to CSV, so the line below is optional.
        # source_format=bigquery.SourceFormat.csv,
        field_delimiter="^",
    )
    uri = f"gs://{PIPELINE_BUCKET_NAME}/extracted_data/Site.txt"

    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = bq_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows in sads_site table.".format(destination_table.num_rows))


# Generic function to move file from source bucket to target bucket
def move_file(source_bucket, destination_bucket, source_files: list, destination_files: list, delete=None):
    for source_path, destination_path in zip(source_files, destination_files):
        source_blob = source_bucket.blob(source_path)

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_path
        )
        if delete is not None:
            source_bucket.delete_blob(source_path)

        print(
            "File {} in bucket {} moved as {} to bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )


# this function will move all files from AdMedia ingress bucket to lakehouse/archive bucket
def archive_reference_files(duplicate_files_list):
    reference_file_directory_path = "ads-reference"
    # blobs = storage_client_impersonated_ingress.list_blobs(INGRESS_ADMEDIA_BUCKET_NAME,
    #                                                            prefix=reference_file_directory_path)
    # blob_list = [blob.name for blob in blobs]

    # source_list = []
    # for file_name in blob_list:
    #     file_split = file_name.split("/")
    #     if len(file_split) >= 2 and file_split[-1] != '':
    #         source_list.append(file_name)

    # destination_list = source_list.copy()

    # # removing 'ads-reference/' from the file name
    # for index in range(len(destination_list)):
    #     destination_list[index] = destination_list[index].replace("ads-reference/", "")
    # print('Files to be moved:')
    # print(source_list)

    print("ARCHIVE_FILES",duplicate_files_list)
    source_bucket = storage_client_impersonated_ingress.bucket(INGRESS_ADMEDIA_BUCKET_NAME)
    destination_bucket = storage_client_impersonated_lakehouse.bucket(ARCHIVE_BUCKET_NAME)

    move_file(source_bucket, destination_bucket, source_files=duplicate_files_list, destination_files=duplicate_files_list, delete=True)
    return None


def delete_files_in_path(path):
    blobs = storage_client_impersonated_ingress.list_blobs(PIPELINE_BUCKET_NAME,
                                                               prefix=path)
    tar_files_list = [blob.name for blob in blobs]
    stage_bucket = storage_client_impersonated_pipeline.bucket(PIPELINE_BUCKET_NAME)
    for source_path in tar_files_list:
        stage_bucket.delete_blob(source_path)
        print(
            "File {} in bucket {} deleted.".format(
                source_path,
                stage_bucket.name,
            )
        )


def delete_reference_files_from_stage_bucket():
    print("deleting the tar file from pipeline/stage bucket")
    delete_files_in_path("ads-reference")
    print("tar file deleted from pipeline/stage bucket successfully")

    print("deleting the extracted files from pipeline/stage bucket successfully")
    delete_files_in_path("extracted_data")
    print("extracted files deleted from pipeline/stage bucket successfully")

def archive_processed_file(**kwargs):
    xcom_var = kwargs["ti"]
    reference_file_names = xcom_var.xcom_pull(key='scanned_ingress_reference_files')
    # reference_filename_timestamp_latest = get_latest_reference_file(reference_file_names)
    print("PROCESSED_FILES:",reference_file_names)
    archive_reference_files(reference_file_names)

# BQ client with impersonated service client
bq_client = get_bq_impersonated_client(SERVICE_ACCOUNT, LAKEHOUSE_PROJECT_ID)

start = DummyOperator(
    task_id='start',
    trigger_rule='all_success',
    dag=dag
)

# control reference table truncate task        
task_truncate_ctl_reference_files_table = BigQueryExecuteQueryOperator(
task_id='truncate_ctl_reference_files_table',
destination_dataset_table="{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
sql="SELECT * FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` where 1=0".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
impersonation_chain=SERVICE_ACCOUNT,
use_legacy_sql=False,
write_disposition='WRITE_TRUNCATE',
labels=GCP_COST_LABELS_BIGQUERY,
dag=dag
)

task_load_reference_file_names_table = PythonOperator(
    task_id='load_reference_file_names_table',
    python_callable=load_reference_file_names_table,
    retries=4,
    retry_delay=timedelta(minutes=RETRY_MINUTES_FOR_FILE_SCAN),  # 1 in Dev, 45 in Cert & Prod
    email_on_failure=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_update_ctl_reference_files_duplicate_reference_ind = BigQueryExecuteQueryOperator(
    task_id='update_ctl_reference_files_duplicate_reference_ind',
    sql='update_ctl_reference_files_duplicate_reference_ind.sql',
    use_legacy_sql=False,
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "PIPELINE_DATASET_ID": PIPELINE_DATASET_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

task_archive_duplicate_reference_files = PythonOperator(
    task_id='archive_duplicate_reference_files',
    python_callable=archive_duplicate_reference_files,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

task_do_not_archive_reference_files = DummyOperator(
    task_id="do_not_archive_reference_files",
    trigger_rule="all_success",
    dag=dag
)

task_decide_duplicate_reference_files_archive = BranchPythonOperator(
    task_id='decide_duplicate_reference_files_archive',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=decide_duplicate_reference_files_archive
)

scan_ref_tar_in_ingress = PythonOperator(
    task_id='scan_ref_tar_in_ingress',
    python_callable=scan_ingress_bucket_for_reference_files,
    labels=GCP_COST_LABELS_PYTHON_API,
    provide_context=True,
    retries=4,
    retry_delay=timedelta(minutes=RETRY_MINUTES_FOR_FILE_SCAN),  # 1 in Dev, 45 in Cert & Prod
    email_on_failure=True,
    trigger_rule="none_failed",
    dag=dag
)

copy_ref_tar_to_stage_bucket = PythonOperator(
    task_id='copy_ref_tar_to_stage_bucket',
    python_callable=copy_reference_files_from_admedia_ingress_bucket_to_stage_bucket,
    labels=GCP_COST_LABELS_PYTHON_API,
    provide_context=True,
    dag=dag
)

extract_reference_files_from_stage_bucket = PythonOperator(
    task_id='extract_reference_file',
    trigger_rule='all_success',
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=extract_ref_files_in_stage_bucket,
    provide_context=True,
    dag=dag
)

validate_extracted_ref_files = PythonOperator(
    task_id='validate_extracted_ref_files',
    trigger_rule='all_success',
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=validate_extracted_files,
    dag=dag
)

clear_stage_tables = BigQueryExecuteQueryOperator(
    task_id='clear_stage_tables',
    sql='clear_data_in_all_stage_tables.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

purge_old_data_in_target_table_categories = BigQueryExecuteQueryOperator(
    task_id='purge_old_data_in_target_table_categories',
    sql='purge_old_data_in_target_table_categories.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

purge_old_data_in_target_table_searchterm_targeting_vars = BigQueryExecuteQueryOperator(
    task_id='purge_old_data_in_target_table_searchterm_targeting_vars',
    sql='purge_old_data_in_target_table_searchterm_targeting_vars.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

purge_old_data_in_target_table_site = BigQueryExecuteQueryOperator(
    task_id='purge_old_data_in_target_table_site',
    sql='purge_old_data_in_target_table_site.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_advertiser_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_advertiser_data_into_bigquery_from_gcs',
    python_callable=insert_advertiser_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_categories_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_categories_data_into_bigquery_from_gcs',
    python_callable=insert_categories_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_campaign_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_campaign_data_into_bigquery_from_gcs',
    python_callable=insert_campaign_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_campaign_site_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_campaign_site_data_into_bigquery_from_gcs',
    python_callable=insert_campaign_site_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_campaign_targeting_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_campaign_targeting_data_into_bigquery_from_gcs',
    python_callable=insert_campaign_targeting_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_creative_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_creative_data_into_bigquery_from_gcs',
    python_callable=insert_creative_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_creative_position_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_creative_position_data_into_bigquery_from_gcs',
    python_callable=insert_creative_position_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_insertion_order_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_insertion_order_data_into_bigquery_from_gcs',
    python_callable=insert_insertion_order_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_searchterm_targeting_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_searchterm_targeting_data_into_bigquery_from_gcs',
    python_callable=insert_searchterm_targeting_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

insert_site_data_into_bigquery_from_gcs = PythonOperator(
    task_id='insert_site_data_into_bigquery_from_gcs',
    python_callable=insert_site_data_into_bigquery,
    labels=GCP_COST_LABELS_PYTHON_API,
    dag=dag
)

load_advertiser_data_from_stage_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_from_stage_to_work_table',
    sql='load_advertiser_data_from_stage_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_advertiser_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_from_target_to_work_table',
    sql='load_advertiser_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_from_stage_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_from_stage_to_work_table',
    sql='load_campaign_data_from_stage_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_from_target_to_work_table',
    sql='load_campaign_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_targeting_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_campaign_targeting_data_from_target_to_work_table',
    sql='load_campaign_targeting_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_from_stage_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_from_stage_to_work_table',
    sql='load_categories_data_from_stage_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_from_target_to_work_table',
    sql='load_categories_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_site_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_site_data_from_target_to_work_table',
    sql='load_site_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_from_stage_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_from_stage_to_work_table',
    sql='load_creative_data_from_stage_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_from_target_to_work_table',
    sql='load_creative_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_from_stage_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_from_stage_to_work_table',
    sql='load_insertion_order_data_from_stage_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_from_target_to_work_table',
    sql='load_insertion_order_data_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_searchterm_targeting_vars_from_target_to_work_table = BigQueryExecuteQueryOperator(
    task_id='load_searchterm_targeting_vars_from_target_to_work_table',
    sql='load_searchterm_targeting_vars_from_target_to_work_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

clear_work_tables = BigQueryExecuteQueryOperator(
    task_id='clear_work_tables',
    sql='clear_data_in_work_tables.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

start_segregation = PythonOperator(
    task_id='start_segregation',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

end_segregation = PythonOperator(
    task_id='end_segregation',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

start_load_to_target_tables = PythonOperator(
    task_id='start_load_to_target_tables',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

end_load_to_target_tables = PythonOperator(
    task_id='end_load_to_target_tables',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

load_advertiser_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_to_work_table_with_row_type_new',
    sql='load_advertiser_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_advertiser_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_to_work_table_with_row_type_update',
    sql='load_advertiser_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_advertiser_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_to_work_table_with_row_type_delete',
    sql='load_advertiser_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_targeting_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_campaign_targeting_data_to_work_table_with_row_type_new',
    sql='load_campaign_targeting_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_targeting_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_campaign_targeting_data_to_work_table_with_row_type_delete',
    sql='load_campaign_targeting_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_to_work_table_with_row_type_new',
    sql='load_campaign_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_to_work_table_with_row_type_update',
    sql='load_campaign_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_to_work_table_with_row_type_delete',
    sql='load_campaign_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_to_work_table_with_row_type_new',
    sql='load_creative_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_to_work_table_with_row_type_update',
    sql='load_creative_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_to_work_table_with_row_type_delete',
    sql='load_creative_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_to_work_table_with_row_type_new',
    sql='load_insertion_order_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_to_work_table_with_row_type_update',
    sql='load_insertion_order_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_to_work_table_with_row_type_delete',
    sql='load_insertion_order_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_searchterm_targeting_vars_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_searchterm_targeting_vars_to_work_table_with_row_type_new',
    sql='load_searchterm_targeting_vars_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_searchterm_targeting_vars_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_searchterm_targeting_vars_to_work_table_with_row_type_delete',
    sql='load_searchterm_targeting_vars_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_searchterm_targeting_vars_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_searchterm_targeting_vars_to_work_table_with_row_type_update',
    sql='load_searchterm_targeting_vars_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_to_work_table_with_row_type_new',
    sql='load_categories_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_to_work_table_with_row_type_update',
    sql='load_categories_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_to_work_table_with_row_type_delete',
    sql='load_categories_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_site_data_to_work_table_with_row_type_new = BigQueryExecuteQueryOperator(
    task_id='load_site_data_to_work_table_with_row_type_new',
    sql='load_site_data_to_work_table_with_row_type_new.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_site_data_to_work_table_with_row_type_update = BigQueryExecuteQueryOperator(
    task_id='load_site_data_to_work_table_with_row_type_update',
    sql='load_site_data_to_work_table_with_row_type_update.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_site_data_to_work_table_with_row_type_delete = BigQueryExecuteQueryOperator(
    task_id='load_site_data_to_work_table_with_row_type_delete',
    sql='load_site_data_to_work_table_with_row_type_delete.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_effective_time = BigQueryExecuteQueryOperator(
    task_id='load_creative_effective_time',
    sql='load_creative_new_eff_ts_table_with_the_later_campaign_effective_ts_values.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

update_creative_work_table_with_campaign_effective_time = BigQueryExecuteQueryOperator(
    task_id='update_creative_work_table_with_campaign_effective_time',
    sql='update_creative_work_table_with_campaign_effective_time.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_advertiser_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_advertiser_data_to_target_table',
    sql='load_advertiser_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_campaign_data_to_target_table',
    sql='load_campaign_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_campaign_targeting_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_campaign_targeting_data_to_target_table',
    sql='load_campaign_targeting_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_creative_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_creative_data_to_target_table',
    sql='load_creative_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_insertion_order_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_insertion_order_data_to_target_table',
    sql='load_insertion_order_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_site_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_site_data_to_target_table',
    sql='load_site_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_searchterm_targeting_vars_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_searchterm_targeting_vars_data_to_target_table',
    sql='load_searchterm_targeting_vars_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

load_categories_data_to_target_table = BigQueryExecuteQueryOperator(
    task_id='load_categories_data_to_target_table',
    sql='load_categories_data_to_target_table.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

# move_reference_files_from_admedia_ingress_bucket_to_archive_bucket = PythonOperator(
#     task_id='move_reference_files_from_admedia_ingress_bucket_to_archive_bucket',
#     dag=dag,
#     labels=GCP_COST_LABELS_PYTHON_API,
#     python_callable=archive_reference_files
# )

start_post_processing = PythonOperator(
    task_id='start_post_processing',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_purging_message
)

delete_files_from_stage_bucket = PythonOperator(
    task_id='delete_files_from_stage_bucket',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=delete_reference_files_from_stage_bucket
)

end_post_processing = PythonOperator(
    task_id='end_post_processing',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

insert_searchterm_targeting_vars_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_searchterm_targeting_vars_discontinued_rows',
    sql='insert_searchterm_targeting_vars_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_insertion_order_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_insertion_order_discontinued_rows',
    sql='insert_insertion_order_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_creative_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_creative_discontinued_rows',
    sql='insert_creative_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_campaign_targeting_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_campaign_targeting_discontinued_rows',
    sql='insert_campaign_targeting_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_campaign_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_campaign_discontinued_rows',
    sql='insert_campaign_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

insert_advertiser_discontinued_rows = BigQueryExecuteQueryOperator(
    task_id='insert_advertiser_discontinued_rows',
    sql='insert_advertiser_discontinued_rows.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

update_searchterm_targeting_vars_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_searchterm_targeting_vars_discontinued_rows_to_active',
    sql='update_searchterm_targeting_vars_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

update_campaign_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_campaign_discontinued_rows_to_active',
    sql='update_campaign_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
update_campaign_targeting_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_campaign_targeting_discontinued_rows_to_active',
    sql='update_campaign_targeting_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

update_advertiser_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_advertiser_discontinued_rows_to_active',
    sql='update_advertiser_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
update_creative_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_creative_discontinued_rows_to_active',
    sql='update_creative_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)
    
update_insertion_order_discontinued_rows_to_active = BigQueryExecuteQueryOperator(
    task_id='update_insertion_order_discontinued_rows_to_active',
    sql='update_insertion_order_discontinued_rows_to_active.sql',
    use_legacy_sql=False,
    bigquery_conn_id='google_cloud_default',
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

end_insert = PythonOperator(
    task_id='end_insert',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

# open sql script file and prepare the SELECT statement
def get_discontinued_rows_from_sql():
    sql_file = open(REFERENCE_PARAMS_PATH + '/reference/select_stage_discontinued_rows.sql', 'r')
    query = sql_file.read().replace("{{ params.PIPELINE_PROJECT_ID }}", PIPELINE_PROJECT_ID)
    sql_file.close()
    return query

# get the error count from wads_aafs_optin_error table for errors from SQLs L 06 to L 12
def get_discontinued_rows():
    discontinued_rows = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    discontinued_rows_query = get_discontinued_rows_from_sql()
    print("query:" + discontinued_rows_query)
    discontinued_rows_query_result = bq_client.query(discontinued_rows_query, location="US", job_config=job_config)
    if discontinued_rows_query_result.result().total_rows == 0:
        print("No results")
    elif discontinued_rows_query_result.result().total_rows == 1:
        for row in discontinued_rows_query_result.result():
            print("discontinued_rows stage tables = " + str(row.discontinued_rows))
            discontinued_rows = row.discontinued_rows
    if discontinued_rows > 0:
        return "start_update_target_tables"
    else:
        return "start_load_to_target_tables"
    return discontinued_rows

# get the column names from metadata information_schema table
def get_column_names(table_name, project_name, dataset_name):
    sql_queries = {"wads_get_advertiser_discontinued_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('advertiser_cd','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_creative_discontinued_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('creative_id','campaign_cd','site_cd','page_cd','position_cd','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_campaign_discontinued_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('campaign_id','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_campaign_targeting_discontinued_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('campaign_cd','targeting_type','targeting_variable_cd','targeting_value','targeting_operator','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_insertion_order_discontinued_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('insertion_order_cd','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_searchterm_targeting_vars_discontnd_rows":"select column_name from {project_name}.{dataset_name}.INFORMATION_SCHEMA.COLUMNS \
    where table_name='{table_name}' and column_name in ('targeting_variable_cd','effective_ts','discontinue_ts')".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name)
    } 
    
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)    
    column_name_query = sql_queries[table_name]
    print("query:" + column_name_query)
    column_name_results = bq_client.query(column_name_query, location="US", job_config=job_config)
    results = column_name_results.result()
    return [result.column_name for result in results]

# prepare data from related wads table and email and send email with attachment    
def prepare_send_mail_with_attachment(table_name, project_name, dataset_name, lakehouse_table_name):
    print("LAKEHOUSE_TABLE_NAME",lakehouse_table_name)
        
    sql_queries = {"wads_get_advertiser_discontinued_rows":"select advertiser_cd,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_creative_discontinued_rows":"select creative_id,campaign_cd,site_cd,page_cd,position_cd,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_campaign_discontinued_rows":"select campaign_id,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_campaign_targeting_discontinued_rows":"select campaign_cd,targeting_type,targeting_variable_cd,targeting_value,targeting_operator,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_insertion_order_discontinued_rows":"select insertion_order_cd,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name),
    "wads_get_searchterm_targeting_vars_discontnd_rows":"select targeting_variable_cd,effective_ts,discontinue_ts \
    from {project_name}.{dataset_name}.{table_name} LIMIT 100000".format(table_name=table_name, project_name=project_name, dataset_name=dataset_name)
    } 
    
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)    
    data_query = sql_queries[table_name]
    print("query:" + data_query)
    data_query_results = bq_client.query(data_query, location="US", job_config=job_config)
    results = data_query_results.result()

    data = []
    columns = get_column_names(table_name, project_name, dataset_name)
    data.append(columns)

    for result in results:
        data.append([result[str(col)] for col in columns])
    
    subject = """%s Failure for ads-reference ADS:REFERENCE-LOAD-GCP %s""" %(ENVIRONMENT, DAG_NAME)
    
    body = """<p style='color:#FF0000'>*** This email has been generated automatically - DO NOT reply directly ***</p>"""
    body += """<strong>Warning</strong>: This AdMedia DAG, %s with asset tag ADS:REFERENCE-LOAD-GCP, 
    has auto-detected <strong>incomplete source data</strong> (which caused BigQuery target table data rows to be mistakenly 
    discontinued in <strong>yesterday’s</strong> load) and has automatically re-activated these specific rows. </br></br>""" %(DAG_NAME)
    
    body += """Due to this data issue, the DAG needs to rerun from the beginning to recalculate the stage table data to be processed. 
    To fully automate this rerun and avoid manual intervention, the last step of this DAG will trigger another instance of this same 
    DAG to start <u><i>after</i></u> this DAG has finished; the new instance will process today’s incoming Reference Load file.  
    <strong>Note</strong>: In this job’s DAG description section, the “max_active_runs” option is set to 1 to enforce that only one instance of the 
    DAG will run at a time.  </br></br>"""
    
    body += """<strong>Impacted Table</strong>: %s.%s.%s </br></br>""" % (LAKEHOUSE_PROJECT_ID,LAKEHOUSE_DATASET_ID,lakehouse_table_name)
    
    body += """<strong>Action</strong>: Please refer to the attached file for this table’s affected rows for investigation into removing 
    the occurrence of the incomplete incoming data, which caused the erroneous target data row discontinuations. </br></br>"""
    
    body += """<strong>Note</strong>: If the number of affected rows is especially large, only the first 100,000 rows will be included. 
    The entire set of data can be viewed in the corresponding %s.%s work table, <strong>but only until the job runs next</strong>, 
    which is likely tomorrow. </br></br>""" % (dataset_name,table_name)
    

    send_mail_with_attachment(data, subject, body, table_name)

# send email with error data file attachment
def send_mail_with_attachment(file_data, subject, body, table_name):    
    try:
        Path("/home/airflow/gcs/data/").mkdir(parents=True, exist_ok=True) #create dir if doesnt exists
        
        with open('/home/airflow/gcs/data/{table_name}.csv'.format(table_name=table_name), 'w') as f:
            writer = csv.writer(f, delimiter='|')       
            for data in file_data:
                writer.writerow(data)
        shutil.make_archive('/home/airflow/gcs/data/' + '{table_name}.sabre'.format(table_name=table_name), 'zip', '/home/airflow/gcs/data/', '{table_name}.csv'.format(table_name=table_name))
        send_email(WARNING_RECIPIENT_EMAIL, subject, body, ['/home/airflow/gcs/data/{table_name}.sabre.zip'.format(table_name=table_name)])
    except Exception as e:  #send error in the task log and prevent the job failing
        print("ERROR:", str(e))
        raise e
    finally: #once email sent files will be removed from Airflow server
        os.remove('/home/airflow/gcs/data/{table_name}.csv'.format(table_name=table_name))
        os.remove('/home/airflow/gcs/data/{table_name}.sabre.zip'.format(table_name=table_name))

# parent function to check error data in advertiser table and email with error data as file attachment
def check_and_email_wads_discontinued_rows(**kwargs):
    table_name = kwargs["table_name"]
    lakehouse_table_name = kwargs["lakehouse_table_name"]
    sql_queries = {"wads_get_advertiser_discontinued_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_advertiser_discontinued_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
"wads_get_creative_discontinued_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_creative_discontinued_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
"wads_get_campaign_discontinued_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_campaign_discontinued_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
"wads_get_campaign_targeting_discontinued_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_campaign_targeting_discontinued_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
"wads_get_insertion_order_discontinued_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_insertion_order_discontinued_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID),
"wads_get_searchterm_targeting_vars_discontnd_rows":"SELECT COUNT(*) discontinued_rows \
FROM {PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.wads_get_searchterm_targeting_vars_discontnd_rows".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)
}
    discontinued_rows = 0
    job_config = bigquery.QueryJobConfig(labels=GCP_COST_LABELS_BIGQUERY)
    discontinued_rows_query = sql_queries[table_name]
    print("query:" + discontinued_rows_query)
    discontinued_rows_query_result = bq_client.query(discontinued_rows_query, location="US", job_config=job_config)
    results = discontinued_rows_query_result.result()
    for result in results:
        if result.discontinued_rows > 0:
            prepare_send_mail_with_attachment(table_name, PIPELINE_PROJECT_ID, PIPELINE_DATASET_ID, lakehouse_table_name)
        else:
            raise AirflowSkipException

# checking ctl_reference_files table for any other reference files needs to be processed, based on that DAG trigger or stop tasks executed    
def check_ctl_reference_files_table_for_reference_files_to_process():
    select_query = "SELECT reference_file_name FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` " \
            "WHERE file_name_ts_integer IN (SELECT MIN(file_name_ts_integer) " \
            "FROM `{PIPELINE_PROJECT_ID}.{PIPELINE_DATASET_ID}.ctl_reference_files` CRFI " \
                "WHERE CRFI.duplicate_reference_ind='N' AND CRFI.processed_flag='N')".format(PIPELINE_PROJECT_ID=PIPELINE_PROJECT_ID,PIPELINE_DATASET_ID=PIPELINE_DATASET_ID)

    result = execute_query(bq_client, select_query)
    print("REFERENCE_FILES_TO_BE_PROCESSED-COUNT:",result.total_rows)
    if result.total_rows != 0:
        trigger_dag_success_email()
        return 'trigger_dag_rerun'
    else:
        return "stop_dag_rerun"

task_get_discontinued_rows = BranchPythonOperator(
    task_id='task_get_discontinued_rows',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=get_discontinued_rows
)

start_update_target_tables = PythonOperator(
    task_id='start_update_target_tables',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

end_update_target_tables = PythonOperator(
    task_id='end_update_target_tables',
    dag=dag,
    trigger_rule='none_failed_or_skipped',
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

trigger_dag_rerun = TriggerDagRunOperator(
    task_id="trigger_dag_rerun",
    trigger_dag_id=DAG_NAME,
    trigger_rule='none_failed_or_skipped',
    wait_for_completion=False,
    dag=dag
)

check_and_email_advertiser_discontinued_rows = PythonOperator(
    task_id='check_and_email_advertiser_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_advertiser_discontinued_rows", "lakehouse_table_name":"advertiser"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

check_and_email_creative_discontinued_rows = PythonOperator(
    task_id='check_and_email_creative_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_creative_discontinued_rows", "lakehouse_table_name":"creative"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

check_and_email_campaign_discontinued_rows = PythonOperator(
    task_id='check_and_email_campaign_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_campaign_discontinued_rows", "lakehouse_table_name":"campaign"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

check_and_email_campaign_targeting_discontinued_rows = PythonOperator(
    task_id='check_and_email_campaign_targeting_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_campaign_targeting_discontinued_rows", "lakehouse_table_name":"campaign_targeting"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

check_and_email_insertion_order_discontinued_rows = PythonOperator(
    task_id='check_and_email_insertion_order_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_insertion_order_discontinued_rows", "lakehouse_table_name":"insertion_order"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

check_and_email_searchterm_targeting_vars_discontinued_rows = PythonOperator(
    task_id='check_and_email_searchterm_targeting_vars_discontinued_rows',
    dag=dag,
    op_kwargs={"table_name":"wads_get_searchterm_targeting_vars_discontnd_rows", "lakehouse_table_name":"searchterm_targeting_vars"},
    provide_context=True,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_and_email_wads_discontinued_rows
)

move_reference_files_from_admedia_ingress_bucket_to_archive_bucket = PythonOperator(
    task_id='archive_processed_file',
    python_callable=archive_processed_file,
    labels=GCP_COST_LABELS_PYTHON_API,
    provide_context=True,
    dag=dag
)

task_update_ctl_reference_files_processed_flag = BigQueryExecuteQueryOperator(
    task_id='update_ctl_reference_files_processed_flag',
    sql='update_ctl_reference_files_processed_flag.sql',
    use_legacy_sql=False,
    provide_context=True,
    location=BQ_DATASET_LOCATION,
    impersonation_chain=SERVICE_ACCOUNT,
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID, "PIPELINE_DATASET_ID": PIPELINE_DATASET_ID},
    labels=GCP_COST_LABELS_BIGQUERY,
    dag=dag)

task_check_ctl_reference_files_table_for_reference_files_to_process = BranchPythonOperator(
    task_id='check_ctl_reference_files_table_for_reference_files_to_process',
    dag=dag,
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=check_ctl_reference_files_table_for_reference_files_to_process
)

stop_dag_run = DummyOperator(
    task_id="stop_dag_rerun",
    trigger_rule="all_success",
    dag=dag
)

end = PythonOperator(
    task_id='end',
    dag=dag,
    trigger_rule="none_failed",
    labels=GCP_COST_LABELS_PYTHON_API,
    python_callable=print_process_message
)

# Main DAG Tasks
start >> task_truncate_ctl_reference_files_table >> task_load_reference_file_names_table >> task_update_ctl_reference_files_duplicate_reference_ind >> task_decide_duplicate_reference_files_archive >> [task_archive_duplicate_reference_files,task_do_not_archive_reference_files] >> scan_ref_tar_in_ingress >> delete_files_from_stage_bucket >> copy_ref_tar_to_stage_bucket >> extract_reference_files_from_stage_bucket
extract_reference_files_from_stage_bucket >> validate_extracted_ref_files >> clear_stage_tables
clear_stage_tables >> [
    insert_advertiser_data_into_bigquery_from_gcs,
    insert_categories_data_into_bigquery_from_gcs,
    insert_campaign_data_into_bigquery_from_gcs,
    insert_campaign_site_data_into_bigquery_from_gcs,
    insert_campaign_targeting_data_into_bigquery_from_gcs,
    insert_creative_data_into_bigquery_from_gcs,
    insert_creative_position_data_into_bigquery_from_gcs,
    insert_insertion_order_data_into_bigquery_from_gcs,
    insert_searchterm_targeting_data_into_bigquery_from_gcs,
    insert_site_data_into_bigquery_from_gcs] >> clear_work_tables

clear_work_tables >> [
    load_advertiser_data_from_stage_to_work_table,
    load_advertiser_data_from_target_to_work_table,
    load_campaign_data_from_stage_to_work_table,
    load_campaign_data_from_target_to_work_table,
    load_campaign_targeting_data_from_target_to_work_table,
    load_site_data_from_target_to_work_table,
    load_categories_data_from_stage_to_work_table,
    load_categories_data_from_target_to_work_table,
    load_creative_data_from_stage_to_work_table,
    load_creative_data_from_target_to_work_table,
    load_insertion_order_data_from_stage_to_work_table,
    load_insertion_order_data_from_target_to_work_table,
    load_searchterm_targeting_vars_from_target_to_work_table] >> start_segregation

start_segregation >> [
    load_advertiser_data_to_work_table_with_row_type_new,
    load_advertiser_data_to_work_table_with_row_type_update,
    load_advertiser_data_to_work_table_with_row_type_delete,

    load_campaign_data_to_work_table_with_row_type_new,
    load_campaign_data_to_work_table_with_row_type_update,
    load_campaign_data_to_work_table_with_row_type_delete,

    load_campaign_targeting_data_to_work_table_with_row_type_new,
    load_campaign_targeting_data_to_work_table_with_row_type_delete,

    load_creative_data_to_work_table_with_row_type_new,
    load_creative_data_to_work_table_with_row_type_update,
    load_creative_data_to_work_table_with_row_type_delete,

    load_insertion_order_data_to_work_table_with_row_type_new,
    load_insertion_order_data_to_work_table_with_row_type_update,
    load_insertion_order_data_to_work_table_with_row_type_delete,

    load_searchterm_targeting_vars_to_work_table_with_row_type_new,
    load_searchterm_targeting_vars_to_work_table_with_row_type_update,
    load_searchterm_targeting_vars_to_work_table_with_row_type_delete,

    load_categories_data_to_work_table_with_row_type_new,
    load_categories_data_to_work_table_with_row_type_update,
    load_categories_data_to_work_table_with_row_type_delete,

    load_site_data_to_work_table_with_row_type_new,
    load_site_data_to_work_table_with_row_type_update,
    load_site_data_to_work_table_with_row_type_delete

] >> end_segregation

end_segregation >> load_creative_effective_time >> update_creative_work_table_with_campaign_effective_time

update_creative_work_table_with_campaign_effective_time >> [insert_searchterm_targeting_vars_discontinued_rows,
    insert_insertion_order_discontinued_rows,
    insert_creative_discontinued_rows,
    insert_campaign_targeting_discontinued_rows,
    insert_campaign_discontinued_rows,
    insert_advertiser_discontinued_rows
    ] >> end_insert
    
end_insert >> task_get_discontinued_rows >> [start_load_to_target_tables, start_update_target_tables]

# Discontinued rows check and email, update the target tables tasks
start_update_target_tables >> check_and_email_advertiser_discontinued_rows >> update_advertiser_discontinued_rows_to_active >> end_update_target_tables
start_update_target_tables >> check_and_email_creative_discontinued_rows >> update_creative_discontinued_rows_to_active >> end_update_target_tables
start_update_target_tables >> check_and_email_campaign_discontinued_rows >> update_campaign_discontinued_rows_to_active >> end_update_target_tables
start_update_target_tables >> check_and_email_campaign_targeting_discontinued_rows >> update_campaign_targeting_discontinued_rows_to_active >> end_update_target_tables
start_update_target_tables >> check_and_email_insertion_order_discontinued_rows >> update_insertion_order_discontinued_rows_to_active >> end_update_target_tables
start_update_target_tables >> check_and_email_searchterm_targeting_vars_discontinued_rows >> update_searchterm_targeting_vars_discontinued_rows_to_active >> end_update_target_tables

# Rerun the DAG incase of discontinued rows identified
end_update_target_tables >> trigger_dag_rerun >> end

start_load_to_target_tables >> [
    load_advertiser_data_to_target_table,
    load_campaign_data_to_target_table,
    load_campaign_targeting_data_to_target_table,
    load_creative_data_to_target_table,
    load_insertion_order_data_to_target_table,
    load_searchterm_targeting_vars_data_to_target_table,
    load_site_data_to_target_table,
    load_categories_data_to_target_table
] >> end_load_to_target_tables >> start_post_processing

start_post_processing >> [
    purge_old_data_in_target_table_site,
    purge_old_data_in_target_table_categories,
    purge_old_data_in_target_table_searchterm_targeting_vars,
    move_reference_files_from_admedia_ingress_bucket_to_archive_bucket
] >> end_post_processing

end_post_processing >> task_update_ctl_reference_files_processed_flag >> task_check_ctl_reference_files_table_for_reference_files_to_process >> [trigger_dag_rerun,stop_dag_run] >> end