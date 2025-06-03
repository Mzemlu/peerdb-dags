from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime,timedelta
import logging

logger = logging.getLogger('airflow')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

DAG_NAME = 'pg_visits_histories_update'
SCHEDULE = '2 * * * *' # every hour at 2 min past hour, e.g. 6:02, 7:02 etc

SCRIPT_PATH_GET_LAST_ID = '/opt/airflow/dags/sql_scripts/pg_visits_histories_update/visits_histories_get_last_id.sql'
SCRIPT_PATH = '/opt/airflow/dags/sql_scripts/pg_visits_histories_update/visits_histories_update.sql'

with DAG(DAG_NAME,
         default_args=default_args,
         schedule_interval=SCHEDULE,
         catchup=False,
         tags=["test", "internal", "clickhouse"]) as dag:

    def get_last_date():
        clickhouse_hook = ClickHouseHook(
                                        database='ddxfitness_prod_v2', 
                                        clickhouse_conn_id='clickhouse_peerdb_prod')

        with open(SCRIPT_PATH_GET_LAST_ID) as f:
            sql_text = f.read()

        result = clickhouse_hook.execute(sql_text)

        return result[0][0] # max id in final table


    def run_update_script(**context):

        import time

        last_id = context["ti"].xcom_pull(key="return_value", task_ids="get_last_date") # get value from previous task

        logger.info(f"Last id:{last_id}")
                
        clickhouse_hook = ClickHouseHook(
                                        database='ddxfitness_prod_v2', 
                                        clickhouse_conn_id='clickhouse_peerdb_prod')

        with open(SCRIPT_PATH) as f:
            sql_text = f.read()

        script_parameters = {
            "last_id": last_id
        }

        sql_script = sql_text.format(**script_parameters)

        start = time.time()
        result = clickhouse_hook.execute(sql_script)
        end = time.time()

        logger.info(f"Script run for {round(end-start)} seconds.")

    get_last_date_task = PythonOperator(
        task_id = 'get_last_date',
        python_callable = get_last_date
    )

    run_update_script_task = PythonOperator(
        task_id = 'run_update_script',
        python_callable = run_update_script
    )

    get_last_date_task >> run_update_script_task


if __name__ == "__main__":
    dag.test()
