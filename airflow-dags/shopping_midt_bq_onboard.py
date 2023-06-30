#Dag Bucket: sab-eda-airflow-dags
#Shopping_MIDT_PROD_REQUEST_1.5_2021-07-19_MU.txt
import pendulum
import yaml, os
from airflow import DAG
from datetime import timedelta, datetime

from airflow.models import TaskInstance, Variable
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryExecuteQueryOperator,BigQueryValueCheckOperator,BigQueryCheckOperator,BigQueryIntervalCheckOperator,BigQueryIntervalCheckOperator)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTablePartitionExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator


# Vars

GCP_REGION = "us-central1"
JOB_NAME = "shopping-midt-bq-onboard"
BQ_DATASET_LOCATION = "US"
LOCAL_TZ = pendulum.timezone("America/Chicago")


#Get  Airflow variables
ENVIRONMENT = os.getenv("env").upper() #"DEV2" #Variable.get("env").upper()
SHOPPING_MIDT_PARAMS_PATH = "/home/airflow/gcs" #os.environ.get('AIRFLOW_HOME')
#SHOPPING_MIDT_PARAMS_PATH = os.environ.get('AIRFLOW_HOME')



#Read parameters from file function
#Just add a comment and see dag run
def read_parameters_from_file(filename: str, env: str):
    import yaml
    with open(filename, 'r') as file:
        #LogUtils.write_log(f"Processing parameters file {filename}", __file__, LogUtils.LOG_DEBUG)
        params = yaml.full_load(file)
        my_config_dict = params[env]
        return my_config_dict

#read values from yaml and set vars
my_config_values = read_parameters_from_file(SHOPPING_MIDT_PARAMS_PATH + '/dags/shopping-midt/Shopping_MIDT/config.yaml',ENVIRONMENT)
#my_config_values = read_parameters_from_file('/opt/airflow/dags/shopping-midt/Shopping_MIDT/config.yaml',ENVIRONMENT)

DATASET_NAME         = "shopping_MIDT"  #my_config_values['lakehouse_dataset_id']
GCP_PROJECT_ID       = my_config_values['lakehouse_project_id']
VERSION              = my_config_values['version']
EMAIL                = my_config_values['email']
START_DATE           = my_config_values['start_date']
EXTRACT_BUCKET       = my_config_values['extract_bucket']
lakehouse_project_id = my_config_values['lakehouse_project_id']
pipeline_project_id  = my_config_values['pipeline_project_id']
SERVICE_ACCOUNT      = my_config_values['SERVICE_ACCOUNT']
pipeline_bucket      = my_config_values['pipeline_bucket']


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
    dag_id='shopping-midt-bq-onboard',
    default_args=default_args,
    description='Shopping MIDT BQ Onboard job',
    #schedule_interval="23 13 * * *",
    schedule_interval="@once",
    max_active_runs= 1,
    catchup=False
)


create_views_task = BigQueryExecuteQueryOperator(
    task_id='create_views_task',
    #sql= 'Shopping_MIDT/4_ins_stg_tbl_shopresponselite_full_day.sql',
    sql= 'Shopping_MIDT/create_view_flow.sql',
    use_legacy_sql=False,
    location=BQ_DATASET_LOCATION,
    write_disposition="WRITE_APPEND",
    params={"pipeline_project_id": pipeline_project_id,"lakehouse_project_id": lakehouse_project_id},
    impersonation_chain=SERVICE_ACCOUNT,
    dag=dag,
)

load_csv_carrier_config_task = GCSToBigQueryOperator(
    task_id='load_csv_carrier_config_task',
    bucket=pipeline_bucket,
    source_objects=['data/carrier_config.csv'],
    skip_leading_rows=1,
    schema_fields=[
        {'name': 'carrier_cd'            , 'type': 'STRING'   , 'mode': 'NULLABLE'},
        {'name': 'included_robotic_flag' , 'type': 'BOOLEAN'  , 'mode': 'NULLABLE'},
        {'name': 'send_data_start_dt'    , 'type': 'DATE'     , 'mode': 'NULLABLE'},
        {'name': 'send_data_end_dt'      , 'type': 'DATE'     , 'mode': 'NULLABLE'},
        {'name': 'mi_flag'               , 'type': 'BOOLEAN'  , 'mode': 'NULLABLE'},
        {'name': 'request_extract_ind'   , 'type': 'BOOLEAN'  , 'mode': 'NULLABLE'},
        {'name': 'response_extract_ind'  , 'type': 'BOOLEAN'  , 'mode': 'NULLABLE'},
        {'name': 'load_ts'               , 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    ],    
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.carrier_config',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

load_csv_carrier_list_od_task = GCSToBigQueryOperator(
    task_id='load_csv_carrier_list_od_task',
    bucket=pipeline_bucket,
    source_objects=['data/carrier_list_od.csv'],
    skip_leading_rows=1,
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.carrier_list_od',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

load_csv_branded_fare_category_sets_task = GCSToBigQueryOperator(
    task_id='load_csv_branded_fare_category_sets_task',
    bucket=pipeline_bucket,
    source_objects=['data/branded_fare_category_sets.csv'],
    skip_leading_rows=1,
    schema_fields=[
        {'name': 'group_subgroup', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'category_set'  , 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.branded_fare_category_sets',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag) 

load_csv_branded_fare_features_task = GCSToBigQueryOperator(
    task_id='load_csv_branded_fare_features_task',
    bucket=pipeline_bucket,
    source_objects=['data/branded_fare_features.csv'],
    skip_leading_rows=1,
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.branded_fare_features',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

load_csv_country_region_code_task = GCSToBigQueryOperator(
    task_id='load_csv_country_region_code_task',
    bucket=pipeline_bucket,
    source_objects=['data/country_region_code.csv'],
    skip_leading_rows=1,
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.country_region_code',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)

load_csv_groupprefixmapping_task = GCSToBigQueryOperator(
    task_id='load_csv_groupprefixmapping_task',
    bucket=pipeline_bucket,
    source_objects=['data/groupprefixmapping.csv'],
    skip_leading_rows=1,
    schema_fields=[
        {'name': 'groupprefix', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'groupname'  , 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.groupprefixmapping',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag) 

load_csv_subgroupprefixmapping_task = GCSToBigQueryOperator(
    task_id='load_csv_subgroupprefixmapping_task',
    bucket=pipeline_bucket,
    source_objects=['data/subgroupprefixmapping.csv'],
    skip_leading_rows=1,
    schema_fields=[
        {'name': 'subgroupprefix', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'subgroup'      , 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    autodetect=True,
    destination_project_dataset_table=pipeline_project_id +'.shopping_MIDT.subgroupprefixmapping',
    impersonation_chain=SERVICE_ACCOUNT,
    write_disposition='WRITE_TRUNCATE',
    dag=dag)


start_pipeline_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

end_pipeline_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Pipeline
start_pipeline_task>>  load_csv_carrier_config_task >> load_csv_carrier_list_od_task >> load_csv_branded_fare_category_sets_task >> load_csv_branded_fare_features_task >> load_csv_country_region_code_task >> load_csv_groupprefixmapping_task >> load_csv_subgroupprefixmapping_task >> create_views_task >>  end_pipeline_task
