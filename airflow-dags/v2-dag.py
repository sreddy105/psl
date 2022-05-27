

"""
EMAIL
"""

def send_email(transaction_dt , missing_file_count):
    body = """<b> failure in the transaction data: %s 
                || the missing files count are :  %s</b></br>""" %(str(transaction_dt), str(missing_file_count))
    body += """<b> Actions:</b>"""
    body += """ <ul>
                    <li>Run this list`:
                        <ul>
                            <li>SELECT * from table</li>
                            <li> NOTE: this table is cleared</li>
                        </ul>
                    </li>
                    <li>CRATE Xandr ticket</li>
                </ul>
    
    """



"""
Ending dag logic

now first check the manifest staging table.

update flag in manifest staging table.
"""
from airflow.operators.python import BranchPythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def get_transaction_dt():
    return "dt"


def run_query(client, query):
    query_job = client.query(query)
    results = query_job.results()

    return results


def update_processed_flag_manifest_stg(transaction_dt):
    update_query = "UPDATE table `mineral-order-337219.test_dags.manifest_stg` " \
                   "SET processed_flag = 'Y' " \
                   "WHERE transaction_dt='{transaction_dt}'".format(transaction_dt=transaction_dt)

    result = run_query(update_query)


def check_for_transaction_date():
    select_query = "SELECT transaction_dt from `mineral-order-337219.test_dags.manifest_stg` " \
                   "WHERE processed_flag='N' "

    result = run_query(select_query)

    if result.total_rows != 0:
        return 'trigger_dependent_dag'
    return 'stop_dag_run_op'


"""
tasks
"""


start = DummyOperator(
    task_id="start",
    trigger_rule="all_success",
    dag=dag
    )


decide_dag_rerun = BranchPythonOperator(
    task_id='decide_dag_rerun_op',
    python_callable=check_for_transaction_date
)

trigger_dag_run_again = TriggerDagRunOperator(
    task_id="trigger_dag_run",
    trigger_dag_id="dag_id",
    wait_for_completion=True
)


stop_dag_runs = DummyOperator(
    task_id="stop_dag_run_op",
    trigger_rule="all_success",
    dag=dag
    )


end = DummyOperator(
    task_id="end",
    trigger_rule="all_success",
    dag=dag
    )


start >> decide_dag_rerun >> []
