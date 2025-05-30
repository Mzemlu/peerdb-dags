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
    dag_id='pg_bi_change_completed_status_transaction',
    default_args=default_args,
    schedule_interval='0 0 * * *',  # Каждый день 
    catchup=False,
    tags=['clickhouse', 'transactions']
) as dag:

    def insert_new_completed_status_changes():
        ch_hook = ClickHouseHook(
            database='ddxfitness_prod_v2',
            clickhouse_conn_id='clickhouse_peerdb_prod'
        )
        
        # Выполняем основной insert
        script_path = '/opt/airflow/dags/sql_scripts/pg_bi_change_completed_status_transaction/increment_load.sql'
        with open(script_path, 'r') as f:
            sql_text = f.read()

        logger.info("Starting incremental insert of new 'completed' transitions.")
        ch_hook.execute(sql_text)

        # Подсчет вставленных строк за последние сутки
        date_filter = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        count_query = f"""
            SELECT count(*) 
            FROM ddxfitness_prod_v2.bi_change_completed_status_transaction 
            WHERE ns_version_updated_at >= toDateTime('{date_filter} 00:00:00')
        """

        conn = ch_hook.get_conn()
        result = conn.execute(count_query)
        count = result[0][0] if result else 0

        if count > 0:
            logger.info(f"Inserted {count} new records.")
        else:
            logger.info("No new records inserted.")
        
        logger.info("Completed incremental insert.")

    insert_task = PythonOperator(
        task_id='incremental_insert_task',
        python_callable=insert_new_completed_status_changes
    )

    insert_task
