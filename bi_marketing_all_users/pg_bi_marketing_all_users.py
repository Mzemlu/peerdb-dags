from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime,timedelta
import logging

logger = logging.getLogger('airflow')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

DAG_NAME = 'pg_bi_marketing_all_users'
SCHEDULE = '7 * * * *'

AIRFLOW_HOME = 'AIRFLOW_HOME'
DAGS_SCRIPTS = f'/dags/sql_scripts/{DAG_NAME}/'

SCRIPTS = ['bi_marketing_all_new.sql']
BACKFILL_DAYS = int(Variable.get("backfill_mobile"))

# построение полного пути до sql файлов
def make_path(script_name):
    import os
    
    airflow_home = os.environ[AIRFLOW_HOME]
    
    return airflow_home + DAGS_SCRIPTS + script_name


with DAG(DAG_NAME,
         default_args=default_args,
         schedule_interval=SCHEDULE,
         catchup=False,
         tags=["test", "internal", "clickhouse"]) as dag:


    def run_script_clickhouse(script_name=None):

        import time

        clickhouse_hook = ClickHouseHook(
                                        database='ddxfitness_prod_v2', 
                                        clickhouse_conn_id='clickhouse_peerdb_prod')

        if script_name:

            with open(make_path(script_name)) as f:
                sql_text = f.read()
            
            logger.info(f"Running script {script_name}")

            date_to = datetime.now().date()
            date_from = date_to + timedelta(-BACKFILL_DAYS)

            script_parameters = {
                "date_from": date_from.strftime('%Y-%m-%d'),
                "date_to": date_to.strftime('%Y-%m-%d')
            }

            sql_script = sql_text.format(**script_parameters)

            sql_scripts = [script for script in sql_script.split(';') if len(script.strip())>0]

            start = time.time()
            result = clickhouse_hook.execute(sql_scripts)
            end = time.time()

            logger.info(f"script {script_name} run for {round(end-start)} seconds.")
        
        else:
            logger.warning(f"No script name supplied")

    #главный таск
    run_main_clickhouse_task = PythonOperator(
        task_id = 'run_main_script',
        python_callable = run_script_clickhouse,
        op_kwargs={"script_name":SCRIPTS[0]},
    )
    
    run_main_clickhouse_task


if __name__ == "__main__":
    dag.test()
