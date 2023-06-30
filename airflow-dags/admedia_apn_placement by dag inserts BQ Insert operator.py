# --------------------------------------------------------------------------------
# Load The Dependencies
# --------------------------------------------------------------------------------

import datetime
import io
import logging

from airflow import models
from airflow.models import DAG
from airflow.operators import dummy_operator
from airflow.operators import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
        BigQueryInsertJobOperator
)


# Project ID's
BQ_PROJECT_ID = "sab-dev-dap-common-4288"
PIPELINE_PROJECT_ID = "sab-dev-dap-common-4288"
LAKEHOUSE_PROJECT_ID = "sab-dev-dap-common-4288"
BQ_DATASET_LOCATION = "US"

#INSERT_ROWS_QUERY = """insert into `sab-dev-dap-common-4288.promospots_ads_stg.placements` values (17837931,'CERTSabre Cruises SC Home Page x01','x01','GPSI','CERTSabre Cruises SC Home Page','APN')"""

#INSERT_ROW_QUERY=(
#        f"INSERT into `sab-dev-dap-common-4288.promospots_ads_stg.placements` VALUES "
#        f"(17837931,'CERTSabre Cruises SC Home Page x01','x01','GPSI','CERTSabre Cruises SC Home Page','APN');"
#    )

# --------------------------------------------------------------------------------
# Set default arguments
# --------------------------------------------------------------------------------

yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    
default_args = {
    'owner': 'airflow',
    'start_date': yesterday,
    'depends_on_past': False,
    'email': ['tracy.spears@sabre.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
    #'retry_delay': datetime.timedelta(minutes=2),
}

# --------------------------------------------------------------------------------
# Main DAG
# --------------------------------------------------------------------------------

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.


with models.DAG(
        #'odi_load_transform_airflow_operator',
        dag_id='admedia_apn_placement_by_dag_inserts',
        default_args=default_args,
        schedule_interval=None) as dag:
    
    start = dummy_operator.DummyOperator(
        task_id='start',
        trigger_rule='all_success')

    end = dummy_operator.DummyOperator(
        task_id='end',
        trigger_rule='all_success')
        
    Insert_Placements = BigQueryOperator(
       task_id='Insert_Placements',
       sql="""SELECT COUNT(*) FROM `sab-dev-dap-common-4288.promospots_ads_stg.placements`""",
       #configuration={
       # "query": {
       #     "query": """insert into `sab-dev-dap-common-4288.promospots_ads_stg.placements` values (17837931,'CERTSabre Cruises SC Home Page x01','x01','GPSI','CERTSabre Cruises SC Home Page','APN')""",
       #     "useLegacySql": False,
       #     }
       # },
       location=BQ_DATASET_LOCATION,
       write_disposition="WRITE_APPEND"
       )  

    start >> Insert_Placements >> end