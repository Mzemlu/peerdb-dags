from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='pg_bi_accounting_transaction_new_version_dag',
    default_args=default_args,
    schedule_interval='0 * * * *',  # каждый час
    catchup=False,
    tags=['clickhouse', 'transactions']
) as dag:

    def incremental_insert():
        ch_hook = ClickHouseHook(
            database='ddxfitness_prod_v2',
            clickhouse_conn_id='clickhouse_peerdb_prod'
        )

        script_path = '/opt/airflow/dags/sql_scripts/pg_bi_accounting_transaction_new_version/incremental_insert.sql'
        with open(script_path, 'r') as f:
            sql_text = f.read()

        logger.info("Executing incremental insert for bi_accounting_transaction_new_version_new...")
        ch_hook.execute(sql_text)
        logger.info("Insert completed.")

    insert_task = PythonOperator(
        task_id='incremental_insert_task',
        python_callable=incremental_insert
    )

    insert_task
