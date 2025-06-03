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
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='pg_inflow_and_outflow_marketing_full_dag',
    default_args=default_args,
    schedule_interval='0 3 * * *',  
    catchup=False,
    tags=['clickhouse', 'marketing']
) as dag:

    def full_reload():
        ch = ClickHouseHook(
            clickhouse_conn_id='clickhouse_peerdb_prod',
            database='ddxfitness_prod_v2'
        )
        script_path = '/opt/airflow/dags/sql_scripts/pg_inflow_and_outflow_marketing/full_replace_inflow_and_outflow_marketing.sql'
        with open(script_path, 'r') as f:
            sql_text = f.read()
        for statement in sql_text.split(';'):
            if statement.strip():
                logger.info(f"Executing: {statement[:100]}...")
                ch.execute(statement.strip())

    run_task = PythonOperator(
        task_id='replace_table_task',
        python_callable=full_reload
    )

    run_task
