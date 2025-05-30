from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 7),
    'retries': 1,
}

DAG_NAME = 'pg_agg_24h_clubs_sessions_update'
# Запуск в 5-ю минуту каждого часа
SCHEDULE = '5 * * * *'
SCRIPT_PATH = '/opt/airflow/dags/sql_scripts/pg_agg_24h_clubs_sessions/agg_24h_incr_update.sql'

with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    tags=["24h", "clubs", "clickhouse"]
) as dag:

    def run_incr_update(**context):
        clickhouse_hook = ClickHouseHook(
            database='ddxfitness_prod_v2',
            clickhouse_conn_id='clickhouse_peerdb_prod'
        )

        with open(SCRIPT_PATH, 'r', encoding='utf-8') as f:
            sql_text = f.read()

        exec_date_utc = context['execution_date']
        local_exec_date = exec_date_utc + timedelta(hours=3)

        last_hour = local_exec_date - timedelta(hours=1)
        this_hour = local_exec_date

        date_from_str = last_hour.strftime('%Y-%m-%d %H:00:00')
        date_to_str = this_hour.strftime('%Y-%m-%d %H:00:00')

        logger.info(f"Recomputing for interval {date_from_str} -- {date_to_str} (Moscow)")

        sql_script = sql_text.format(
            date_from=date_from_str,
            date_to=date_to_str
        )

        queries = [q.strip() for q in sql_script.split(';') if q.strip()]
        for q in queries:
            try:
                result = clickhouse_hook.execute(q)
                rowcount = result.rowcount if result else 0
                logger.info(f"Executed query: {q[:70]}... Inserted {rowcount} rows.")
            except Exception as e:
                logger.error(f"Error executing query: {q[:70]}... - {str(e)}")
                raise

    run_incr_update_task = PythonOperator(
        task_id='run_incr_update_task',
        python_callable=run_incr_update,
        provide_context=True
    )
