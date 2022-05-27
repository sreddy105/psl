# import json
# import requests
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryBaseCursor
# from datetime import datetime
# resp = {}
#
#
# def function_post():
#     api_response = json.loads(api_response)
#     url = "http://google.com/data"
#     data = {'keyword': keyword}
#     token = data['response']['token']
#     headers = {'Authorization': "Bearer {}".format(token)}
#     r = requests.post(url, headers=headers, data=data)
#     print(r.text)
#     r = json.loads(r.text)
#     report_id = r['response']['report_id']
#     return {'token': token, 'report_id': report_id}  # new
#
#
# def download_data(**kwargs):
#     x_Comm_var = kwargs['ti']
#     response = x_Comm_var.xcomm_pull(task_ids='function_post')
#     report_id = response['report_id']  # new
#     token = response['token']  # new
#     url = "https://google.com" + report_id
#     print(url)  # new
#     headers = {'Authorization': "Bearer {}".format(token)}
#     r = requests.get(url, headers=headers)
#     data = r.text
#     print(data)
#     return data
#
#     # report_id = "rows"
#     # url = "anyurl" + report_id
#     # response = requests.post(url)
#     # data = response.content
#     # print(data)
#
#
# def load_bq(**kwargs):
#     x_Comm_var = kwargs['ti']
#     data = x_Comm_var.xcomm_pull(task_ids='download_data')
#     data_filter = list(filter(bool, data.splitlines()))
#     columns = data_filter[0].split(",")
#     values = data_filter[1].split(",")
#     final_data = {}
#     for key, value in zip(columns, values):
#         final_data[key] = value
#     sql_Statement = "INSERT INTO {schemaName} " \
#                     "(dataset_name, table_name, transaction_dt, apn_impression_count, load_ts )" \
#                     "SELECT {database_name}, {table_name}, {transaction_dt}, {apn_impression_count}, {load_ts};"
#     sql = sql_Statement.format(schemaName="", database_name="",table_name="",
#                          transaction_dt=final_data['day'],
#                          apn_impression_count =final_data['imps']
#                          , load_ts=datetime.now())
#     bq_operator = BigQueryHook(bigquery_conn_id=bQ_CONN_ID,use_legacy_sql=False )
#     # bq_connection = bq_operator.get_conn() # new
#     # cursor = bq_connection.cursor() # new
#     #cursor.run_query() # new
#     run_query_operator = BigQueryBaseCursor(service=service, project_id="")
#     run_query_operator.run_query(
#         sql="",
#         destination_dataset_table="",
#         allow_large_results=True,
#         use_legacy_sql=False,
#         write_disposition='WRITE_APPEND'
#     )
#
#

"""
TESTING
"""

from datetime import datetime

"""
TEST -1
"""
x_Comm_var=None
final_sql = ""
def load_bq(**kwargs):
    """
    REPLACE LINE - 101 at your code
    and inside "run_query" write "sql=sql"
    :return:
    """
    data = x_Comm_var.xcomm_pull()
    csv_data = list(filter(bool, data.splitlines()))
    columns = csv_data.pop(0).split(",")

    dataset_name = "promospots_ads_stq"
    table_name = "transaction_audit_log"
    load_ts = datetime.now()

    csv_table = {}
    for row_no in range(len(columns)):
        csv_table[columns[row_no]] =[val.split(",")[row_no] for val in csv_data]


    values_fmt = "('{dataset_name}', '{table_name}', '{transaction_dt}', '{apn_impression_count}', '{load_ts}'),"
    values_sql = ""
    for row_no in range(len(csv_data)):
        values_sql += values_fmt.format(dataset_name=dataset_name, table_name=table_name,
                                        transaction_dt=csv_table['day'][row_no],
                                        apn_impression_count=csv_table['imps'][row_no],
                                        load_ts=load_ts)
    values_sql = values_sql[:-1]


    sql_Statement = "INSERT INTO `{schemaName}` " \
                    "(dataset_name, table_name, transaction_dt, apn_impression_count, load_ts) " \
                    "VALUES " + values_sql
    sql = sql_Statement.format(schemaName="sab-dev-dab-common-4288.promospots_ads_stq.transaction_audit_log")
    final_sql = sql

    # bq_operator = BigQueryBaseCursor(service="", project_id="",use_legacy_sql=False )
    # service = bq_operator.
    # bq = BigQueryHook(bigquery_conn_id=self.gcp_conn_id, delegate_to=None, use_legacy_sql=True, location='US')
    # pd = BigQueryPandasConnector(bq._get_field('project'), bq.get_service())
    # df = pd.read_gbq(self.query)



a = set()
a.update([1])
a.update([2])
print(a[1])
