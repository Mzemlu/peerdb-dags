from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger('airflow')
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1), 
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

SCHEDULE = '0 7,14 * * *'  # два раза в сутки: 10:00 и 17:00 по МСК

with DAG(
    dag_id='pg_bi_swipe_history_increment',
    default_args=default_args,
    schedule_interval=SCHEDULE,
    catchup=False,
    tags=['clickhouse', 'swipe_history']
) as dag:
    
    def load_increment_data():
        ch_hook = ClickHouseHook(
            database='ddxfitness_prod_v2',
            clickhouse_conn_id='clickhouse_peerdb_prod'
        )
        conn = ch_hook.get_conn()
        
        # 1. Определение max(id) 
        rows = conn.execute("""
            SELECT max(id) 
            FROM ddxfitness_prod_v2.bi_swipe_history
        """)
        last_id = rows[0][0] or 0
        
        date_rows = conn.execute("""
            SELECT max(event_date)
            FROM ddxfitness_prod_v2.bi_swipe_history
        """)
        last_dt = date_rows[0][0]
        
        logger.info(f"Last loaded ID: {last_id}, last event_date: {last_dt}")
        
        # 2. SQL-запрос
        sql_query = f"""
        INSERT INTO ddxfitness_prod_v2.bi_swipe_history
        SELECT
            sh.id,
            toDateTime(sh.event_date + toIntervalHour(3), 'UTC') AS event_date,
            sh.event_type,
            sh.sys_response,
            multiIf(
                club1.id != 0, club1.id,
                club2.id != 0, club2.id,
                club3.id
            ) AS club_id,
            multiIf(
                club1.name != '', club1.name,
                club2.name != '', club2.name,
                club3.name
            ) AS club_name,
            sh.user_id,
            sh.is_success
        FROM
        (
            SELECT
                id,
                event_date,
                event_type,
                sys_response,
                any(club_id) OVER (
                    PARTITION BY user_id, toDate(event_date)
                    ORDER BY id ASC
                ) AS first_club_id,
                lagInFrame(club_id) OVER (
                    PARTITION BY user_id, toDate(event_date)
                    ORDER BY id ASC
                ) AS prev_club_id,
                leadInFrame(club_id) OVER (
                    PARTITION BY user_id, toDate(event_date)
                    ORDER BY id ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS next_club_id,
                club_id,
                user_id,
                is_success
            FROM ddxfitness_prod_v2.pg_card_swipe_histories
            WHERE event_type = 'qr_check'
              AND id > {last_id}
        ) AS sh
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS club1 ON sh.prev_club_id  = club1.id
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS club2 ON sh.next_club_id  = club2.id
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS club3 ON sh.first_club_id = club3.id
        """

        logger.info(f"Inserting new records with ID > {last_id}...")
        conn.execute(sql_query)
        
        # 3. Проверка вставки
        check_rows = conn.execute(f"""
            SELECT count() 
            FROM ddxfitness_prod_v2.bi_swipe_history
            WHERE id > {last_id}
        """)
        new_records = check_rows[0][0]
        logger.info(f"Insertion done. Added {new_records} new records.")
        
        # 4. Оптимизация таблицы
        try:
            now = datetime.now()
            current_month = now.strftime('%Y%m')
            prev_month = (now.replace(day=1) - timedelta(days=1)).strftime('%Y%m')

            logger.info(f"Optimizing partitions: {prev_month} and {current_month}")
            conn.execute(f"""
                OPTIMIZE TABLE ddxfitness_prod_v2.bi_swipe_history
                PARTITION '{prev_month}' FINAL
            """)
            conn.execute(f"""
                OPTIMIZE TABLE ddxfitness_prod_v2.bi_swipe_history
                PARTITION '{current_month}' FINAL
            """)
            logger.info("Optimization completed successfully.")
        except Exception as e:
            logger.warning(f"Optimization error (non-critical): {str(e)}")
        
        logger.info("Processing completed.")
    
    load_task = PythonOperator(
        task_id='load_increment_task',
        python_callable=load_increment_data
    )

    load_task
