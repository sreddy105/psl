from google.cloud import bigquery


def get_impersonate_client():
    pass


def load_file_to_bq():
    table_id = ""
    client  = get_impersonate_client()
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://bucketname/file.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

def create_view():
    client = bigquery.Client() # use impsersonation client

    view_id = "my-project.my_dataset.my_view"
    source_id = "my-project.my_dataset.my_table"
    view = bigquery.Table(view_id)
    view.view_query = f"SELECT name, post_abbr FROM `{source_id}` WHERE name LIKE 'W%'"

    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

task1 = PythonOperator(
    task_id='load_file_to_bq',
    python_callable=load_file_to_bq,
    dag=dag)



