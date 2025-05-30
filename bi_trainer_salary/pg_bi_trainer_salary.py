from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'pg_bi_trainer_salary',
    default_args=default_args,
    schedule_interval='15 1 * * *',  # каждый день в 01:15
    catchup=False,
    tags=['clickhouse', 'trainer_salary']
) as dag:

    def full_refresh_trainer_salary():
        ch_hook = ClickHouseHook(
            database='ddxfitness_prod_v2',
            clickhouse_conn_id='clickhouse_peerdb_prod'
        )

        script_path = '/opt/airflow/dags/sql_scripts/pg_bi_trainer_salary/insert_only_new_timetable_ids.sql'
        with open(script_path, 'r') as f:
            sql_text = f.read()

        statements = [s.strip() for s in sql_text.split(';') if s.strip()]

        logger.info("Starting full refresh for bi_trainer_salary...")

        for st in statements:
            preview = st.replace('\n', ' ')[:100]
            logger.info(f"Executing SQL: {preview}...")
            ch_hook.execute(st)

        logger.info("Done refreshing bi_trainer_salary.")

    load_task = PythonOperator(
        task_id='full_refresh_trainer_salary',
        python_callable=full_refresh_trainer_salary
    )

    load_task
