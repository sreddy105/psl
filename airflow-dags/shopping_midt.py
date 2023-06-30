import pendulum
import yaml, os
from airflow import DAG
from datetime import timedelta, datetime

from airflow.models import TaskInstance, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryExecuteQueryOperator,BigQueryValueCheckOperator,BigQueryCheckOperator,BigQueryIntervalCheckOperator,BigQueryIntervalCheckOperator)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor, BigQueryTableExistenceSensor
from airflow.operators.python_operator import PythonOperator



# Vars

GCP_REGION = "us-central1"
JOB_NAME = "shopping-midt"
BQ_DATASET_LOCATION = "US"
LOCAL_TZ = pendulum.timezone("America/Chicago")

#Get  Airflow variables
ENVIRONMENT = os.getenv("env").upper() #"DEV2" #Variable.get("env").upper()
SHOPPING_MIDT_PARAMS_PATH = "/home/airflow/gcs/dags" #os.environ.get('AIRFLOW_HOME')


#Read parameters from file function
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        #LogUtils.write_log(f"Processing parameters file {filename}", __file__, LogUtils.LOG_DEBUG)
        params = yaml.full_load(file)
        my_config_dict = params[env]
        return my_config_dict

#read values from yaml and set vars
my_config_values = read_parameters_from_file(SHOPPING_MIDT_PARAMS_PATH + '/shopping-midt/Shopping_MIDT/config.yaml',ENVIRONMENT)
LAKEHOUSE_PROJECT_ID     = my_config_values['lakehouse_project_id']
PIPELINE_PROJECT_ID      = my_config_values['pipeline_project_id']
LAKEHOUSE_DATASET_ID     = my_config_values['LAKEHOUSE_DATASET_ID'] #my_config_values['lakehouse_dataset_id']
PIPELINE_DATASET_ID = my_config_values['PIPELINE_DATASET_ID']
VERSION        = my_config_values['version']
EMAIL          = my_config_values['email']
START_DATE     = my_config_values['start_date']
EXTRACT_BUCKET = my_config_values['extract_bucket']
SERVICE_ACCOUNT  = my_config_values['SERVICE_ACCOUNT']

# Default args
default_args = {
    'owner': 'radha.rajavarapu',
    'depends_on_past': True,
    'email': [EMAIL],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': START_DATE,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True
}

# DAG definition
dag = DAG(
    dag_id='shopping-midt',
    default_args=default_args,
    description='Shopping MIDT  job',
    schedule_interval="23 13 * * *",
    #schedule_interval="@daily",
    max_active_runs= 1,
    catchup=True
)

#make sure base references tables already run and are populated
check_ref_data_load = BigQueryTableExistenceSensor(
    task_id='check_ref_data_load',
    project_id=PIPELINE_PROJECT_ID,
    dataset_id='shopping_MIDT_stg_vw',
    table_id='response_lite_daily_src',
    poke_interval=120, #2 minutes
    timeout=60 * 60,   #1 hour
    mode="reschedule",
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag
)




#Request Steps
# step 0
check_airshop_request_partitions = BigQueryTablePartitionExistenceSensor(
    task_id='check_airshop_request_partitions',
    project_id=LAKEHOUSE_PROJECT_ID,
    dataset_id='airshopping',
    table_id='shop_request',
    partition_id='{{ ds_nodash }}', #YYYYMMDD
    poke_interval=120,     #2 minutes
    timeout=60 * 60 * 6 ,  #6 hours
    mode="reschedule",
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag
)

# step 1
run_ins_stg_excludes_task = BigQueryExecuteQueryOperator(
    task_id='run_ins_stg_excludes',
    project_id=LAKEHOUSE_PROJECT_ID,
    sql='Shopping_MIDT/2_ins_stage_excludes.sql',
    use_legacy_sql=False,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

# step 2
run_shop_midt_request_task = BigQueryExecuteQueryOperator(
    task_id='run_shop_midt_request',
    sql='Shopping_MIDT/3_request.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

#Response Steps follows
# step 0 Check for shop_record partitions
check_airshop_response_partitions = BigQueryTablePartitionExistenceSensor(
    task_id='check_airshop_response_partitions',
    project_id=LAKEHOUSE_PROJECT_ID,
    dataset_id='airshopping',
    table_id='shop_record',
    partition_id='{{ ds_nodash }}' + '23', #YYYYMMDDHH
    poke_interval=30,
    timeout=60 * 12,
    mode="reschedule",
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag
)


#Step1:Clear all tables and load source data
run_clear_response_staging_tables = BigQueryExecuteQueryOperator(
    task_id='run_clear_response_staging_tables',
    sql= 'Shopping_MIDT/1_clear_response_staging_tables.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

run_ins_res_src_task = BigQueryExecuteQueryOperator(
    task_id='run_ins_res_src_task',
    #sql= 'Shopping_MIDT/4_ins_stg_tbl_shopresponselite_full_day.sql',
    sql= 'Shopping_MIDT/4_ins_stg_tbl_shopresponselite.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

#step 2: Aggregated data for all carriers with fairIndex and other calculations

run_ins_response_all_carriers = BigQueryExecuteQueryOperator(
    task_id='run_ins_response_all_carriers',
    sql=  'Shopping_MIDT/5_ins_stg_tbl_shop_midt_response_all_carriers.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)


#step 3: Airshopping branded fares and codes


run_ins_air_shopping = BigQueryExecuteQueryOperator(
    task_id='run_ins_air_shopping',
    sql= 'Shopping_MIDT/6_ins_stg_tbl_air_shopping.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

#step 4: Request data for that day


run_ins_shop_request = BigQueryExecuteQueryOperator(
    task_id='run_ins_shop_request',
    sql='Shopping_MIDT/7_ins_stg_tbl_shoprequest_src.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

#step 5:Request and response match

run_ins_stg_shop_response = BigQueryExecuteQueryOperator(
    task_id='run_ins_stg_shop_response',
    sql= 'Shopping_MIDT/8_ins_stg_tbl_shop_midt_response.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

#step 6:Final target load


run_ins_tgt_shop_response = BigQueryExecuteQueryOperator(
    task_id='run_ins_tgt_shop_response',
    sql= 'Shopping_MIDT/9_ins_shop_midt_response.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"PIPELINE_PROJECT_ID": PIPELINE_PROJECT_ID,"LAKEHOUSE_PROJECT_ID": LAKEHOUSE_PROJECT_ID},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)


def getImpersonatedCredentials(target_scopes: list, target_service_account: str):
    from google.auth import _default

    source_credentials, project_id = _default.default(scopes=target_scopes)

    from google.auth import impersonated_credentials

    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=SERVICE_ACCOUNT,
        target_scopes=target_scopes,
        lifetime=600)
    return target_credentials


def GenerateCSVExtract(**kwargs):
    from google.cloud import bigquery
    from google.cloud import storage
    import sys, os


    #BQ_DATASET_NAME = 'shopping_MIDT'

    SHOP_DATE = kwargs['SHOP_DATE'] # '2021-09-20'  # 2021-07-12
    TABLE_NAME = kwargs['TABLE_NAME']
    #CERT testing route
   # FILE_NAME_PREFIX = 'shop_midt_' + TABLE_NAME + '_' + 'CERT' + '_' + VERSION + '_' + SHOP_DATE + '_'
    FILE_NAME_PREFIX = 'shop_midt_' + TABLE_NAME + '_' + ENVIRONMENT + '_' + VERSION + '_' + SHOP_DATE + '_'
    BQ_DATASET_NAME=kwargs['BQ_DATASET_NAME']
    BQ_stg_DATASET_NAME = kwargs['BQ_stg_DATASET_NAME']

    target_project = PIPELINE_PROJECT_ID


    #Passing the credentilas using the JSON credentials file
    #credentials = service_account.Credentials.from_service_account_file("bigquery-training-278108-bd1b9b15e93d.json")
    target_scopes = ['https://www.googleapis.com/auth/cloud-platform']
    target_credentials = getImpersonatedCredentials(target_scopes,SERVICE_ACCOUNT)
    #Derving the bigquery client and assigning the credentials

    bigqueryClient = bigquery.Client(project=target_project,
                                     credentials=target_credentials)

    storage_client = storage.Client(project=target_project,
                            credentials=target_credentials)

    LAKEHOUSE_DATASET_NAME = PIPELINE_PROJECT_ID+"."+ BQ_DATASET_NAME

    #Query to select the required carriers
    carriers_query = """select  carrier_cd from """+LAKEHOUSE_DATASET_NAME+""".carrier_config where '"""+SHOP_DATE+"""' between send_data_start_dt and send_data_end_dt and """+TABLE_NAME+"""_extract_ind = TRUE"""
    #Running the query job in BigQuery and converting the results into a pandas dataframe


    #Converting the dataframe column into a list which acts as a input to the parameterized queries
    carrier_query_job = bigqueryClient.query(carriers_query).to_dataframe(create_bqstorage_client=False)
    carriers_list = carrier_query_job['carrier_cd'].tolist()
    print(carriers_list)
    print(carriers_query)



    PIPELINE_DATASET_NAME = PIPELINE_PROJECT_ID+"."+ BQ_stg_DATASET_NAME
    #For loop to iterate through the carriers list
    for i in carriers_list:
        #query to select the required columns for the request
        query = """ SELECT *  from """+PIPELINE_DATASET_NAME+""".shop_midt_"""+TABLE_NAME+"""_extract 
        where date(shop_dttm) = '"""+SHOP_DATE+"""'
        and carrier_cd = @Names"""

        print(query)
        #In the job config passing the carriers list in the bigquery scalar parameter which we are using that output in SQl where clause
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("Names", "STRING", i),
            ]
        )
        #Running the query job and assigning the results to a pandas dataframe
        #check row count based on that create extract
        query_job = bigqueryClient.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=False)
        #converting the pandas dataframe into a CSV file
        csv_file = query_job.to_csv(FILE_NAME_PREFIX + i + '.csv', index=False)
        #Uploading the generated CSV files to the Google Cloud Storage bucket
        bucket = storage_client.bucket(EXTRACT_BUCKET)
        blob = bucket.blob(FILE_NAME_PREFIX + i + '.csv')
        blob.upload_from_filename(FILE_NAME_PREFIX + i + '.csv')

        #delete local file
        if os.path.exists(FILE_NAME_PREFIX + i + '.csv'):
            os.remove(FILE_NAME_PREFIX + i + '.csv')
        else:
            print("The file " +FILE_NAME_PREFIX + i+" does not exist when trying to delete from local airflow server")

# Generate extracts Response

run_export_request_file = PythonOperator(
    task_id='run_export_request_file',
    python_callable=GenerateCSVExtract,
    op_kwargs={'SHOP_DATE': '{{ds}}', 'TABLE_NAME': 'request','BQ_DATASET_NAME': LAKEHOUSE_DATASET_ID ,'BQ_stg_DATASET_NAME': PIPELINE_DATASET_ID},
    dag=dag, )

export_response_file = PythonOperator(
    task_id='export_response_file',
    python_callable=GenerateCSVExtract,
    op_kwargs={'SHOP_DATE': '{{ds}}', 'TABLE_NAME': 'response','BQ_DATASET_NAME': LAKEHOUSE_DATASET_ID ,'BQ_stg_DATASET_NAME': PIPELINE_DATASET_ID},
    dag=dag, )


end_pipeline_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Real Pipeline
check_ref_data_load >> check_airshop_request_partitions >> run_ins_stg_excludes_task >> run_shop_midt_request_task  >> check_airshop_response_partitions >> run_clear_response_staging_tables >> run_ins_res_src_task >> run_ins_response_all_carriers >> [run_ins_air_shopping , run_ins_shop_request] >> run_ins_stg_shop_response >> run_ins_tgt_shop_response >>  export_response_file >> run_export_request_file >> end_pipeline_task

#Dev2 test pipeline
#run_ins_stg_excludes_task >> run_shop_midt_request_task  >> run_clear_response_staging_tables >> run_ins_res_src_task >> run_ins_response_all_carriers >> [run_ins_air_shopping , run_ins_shop_request] >> run_ins_stg_shop_response >> run_ins_tgt_shop_response >> [run_export_request_file , export_response_file] >> end_pipeline_task
#run_ins_stg_excludes_task >> run_shop_midt_del_partition_task >> (run_shop_midt_request_task  , end_pipeline_task)
