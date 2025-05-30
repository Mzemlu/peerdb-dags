from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow')

DAG_NAME = 'pg_bi_moratorium'

AIRFLOW_HOME = 'AIRFLOW_HOME'
DAGS_SCRIPTS = f'/dags/sql_scripts/{DAG_NAME}/'

SCRIPTS = ['full_load_bi_moratorium_new.sql','bi_moratorium_trial.sql']

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# построение полного пути до sql файлов
def make_path(script_name):
    import os
    
    airflow_home = os.environ[AIRFLOW_HOME]
    
    return airflow_home + DAGS_SCRIPTS + script_name


with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval='15 0 * * *',  # Запуск в 00:15 ежедневно
    catchup=False,
    tags=['clickhouse', 'moratorium']
) as dag:

    def run_script_clickhouse(script_name=None):

        import time

        clickhouse_hook = ClickHouseHook(
                                        database='ddxfitness_prod_v2', 
                                        clickhouse_conn_id='clickhouse_peerdb_prod')

        if script_name:

            with open(make_path(script_name)) as f:
                sql_text = f.read()

            logger.info(f"Running script {script_name}")

            # if there are injected variables - use the following
            # date_to = datetime.now().date()
            # date_from = date_to + timedelta(-BACKFILL_DAYS)

            # script_parameters = {
            #     "date_from": date_from.strftime('%Y-%m-%d'),
            #     "date_to": date_to.strftime('%Y-%m-%d')
            # }

            # sql_script = sql_text.format(**script_parameters)
            sql_script = sql_text
            
            sql_scripts = [s.strip() for s in sql_script.split(';') if s.strip()]
            start = time.time()
            result = clickhouse_hook.execute(sql_scripts)
            end = time.time()

            logger.info(f"script {script_name} run for {round(end-start)} seconds.")
        
        else:
            logger.warning(f"No script name supplied")

    #главный таск
    run_full_load_task = PythonOperator(
        task_id = 'full_load_bi_moratorium_task',
        python_callable = run_script_clickhouse,
        op_kwargs={"script_name":SCRIPTS[0]},
    )

    run_sub_clickhouse_task = PythonOperator(
        task_id = 'trial_bi_moratorium_task',
        python_callable = run_script_clickhouse,
        op_kwargs={"script_name":SCRIPTS[1]},
    )
    
    run_full_load_task >> run_sub_clickhouse_task
