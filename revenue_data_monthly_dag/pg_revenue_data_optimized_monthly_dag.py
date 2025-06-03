from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="pg_revenue_data_optimized_monthly_dag",
    schedule_interval="0 5 * * *",  # ежедневно в 5 утра
    default_args=default_args,
    catchup=False,
    tags=["revenue_data", "monthly", "optimized"]
) as dag:

    @task
    def load_monthly_data():
        ch = ClickHouseHook(
            clickhouse_conn_id="clickhouse_peerdb_prod",
            database="ddxfitness_prod_v2"
        )

        today = datetime.today()

        if today.day <= 2:
            prev_month_end = today.replace(day=1) - timedelta(days=1)
            prev_month_start = prev_month_end.replace(day=1).strftime("%Y-%m-%d")
            prev_month_end_str = prev_month_end.strftime("%Y-%m-%d 23:59:59")

            logging.info(f"Перезагрузка предыдущего месяца: {prev_month_start} - {prev_month_end_str}")

            delete_prev_month_sql = f"""
            ALTER TABLE ddxfitness_prod_v2.revenue_data
            DELETE WHERE date = toStartOfMonth(toDate('{prev_month_start}'))
            """
            ch.execute(delete_prev_month_sql)

            insert_prev_month_sql = generate_insert_query(prev_month_start, prev_month_end_str)
            ch.execute(insert_prev_month_sql)

        current_month_start = today.replace(day=1).strftime("%Y-%m-%d")
        current_month_end = today.strftime("%Y-%m-%d 23:59:59")

        logging.info(f"Перезагрузка текущего месяца: {current_month_start} - {current_month_end}")

        delete_current_month_sql = f"""
        ALTER TABLE ddxfitness_prod_v2.revenue_data
        DELETE WHERE date = toStartOfMonth(toDate('{current_month_start}'))
        """
        ch.execute(delete_current_month_sql)

        insert_current_month_sql = generate_insert_query(current_month_start, current_month_end)
        ch.execute(insert_current_month_sql)

    def generate_insert_query(start_date: str, end_date: str) -> str:
        return f"""
        INSERT INTO ddxfitness_prod_v2.revenue_data (
            date,
            payment_plan_id,
            payment_name,
            club_id,
            club_name,
            city_id,
            club_type,
            open_date,
            total_cost
        )
        WITH user_payment_plan_table AS (
            SELECT *
            FROM ddxfitness_prod_v2.pg_user_payment_plans
            WHERE payment_plan_id NOT IN (111, 1)
        ),
        changing_clubs AS (
            SELECT
                ppcc.user_payment_plan_id,
                ppcc.user_id,
                ppcc.old_club_id,
                toTimezone(ppcc.created_at, 'Europe/Moscow') + INTERVAL 3 HOUR AS changing_club_date,
                ex.start_date
            FROM ddxfitness_prod_v2.pg_payment_plans_changing_clubs ppcc
            INNER JOIN user_payment_plan_table ex ON ppcc.user_payment_plan_id = ex.id
            WHERE toTimezone(ppcc.created_at, 'Europe/Moscow') + INTERVAL 3 HOUR >= start_date
        ),
        plans_with_changing_clubs AS (
            SELECT user_payment_plan_id AS id, changing_club_date AS end_date, old_club_id AS club_id, user_id, 'Changing_club' AS status FROM changing_clubs
            UNION ALL
            SELECT id, start_date, club_id, user_id, 'Changing_club' FROM user_payment_plan_table
            UNION ALL
            SELECT id, end_date, club_id, user_id, status FROM user_payment_plan_table
        ),
        final_extraction AS (
            SELECT
                id,
                lagInFrame(toDate(end_date)) OVER (PARTITION BY id ORDER BY end_date) AS start_date,
                toDate(end_date) AS end_date,
                club_id,
                user_id,
                status
            FROM plans_with_changing_clubs
        ),
        extraction_plus_prev_next AS (
            SELECT *
            FROM (
                SELECT
                    id,
                    club_id,
                    user_id,
                    start_date,
                    status,
                    end_date,
                    row_number() OVER (PARTITION BY user_id, id ORDER BY start_date ASC) AS rnk
                FROM final_extraction
                WHERE start_date IS NOT NULL
            ) t
            WHERE rnk = 1
        ),
        all_transactions_users AS (
            SELECT
                t.user_id AS user_id_t,
                t.updated_at,
                t.total_amount,
                t.user_payment_plan_id,
                s.club_id AS legal_club_id,
                COALESCE(upp.payment_plan_id, 0) AS payment_plan_id,
                CASE
                    WHEN t.provider_id IN (1,2,5,6,7,9,10,12,13,16,17,20,21,24,25,26,27,28,29,30,31,34,37,38) THEN t.total_amount
                    WHEN t.provider_id IN (8,11,14,15,18,19) THEN t.total_amount * -1
                    ELSE 0
                END AS revenue
            FROM ddxfitness_prod_v2.bi_completed_transactions t
            LEFT JOIN ddxfitness_prod_v2.pg_user_payment_plans upp ON t.user_payment_plan_id = upp.id
            LEFT JOIN ddxfitness_prod_v2.pg_club_legal_infos s ON t.club_legal_info_id = s.id
            WHERE payment_plan_id NOT IN (111, 1)
              AND NOT (user_payment_plan_id IS NULL AND legal_club_id = 0)
              AND t.updated_at BETWEEN '{start_date}' AND '{end_date}'
        ),
        final_table AS (
            SELECT
                a.user_id_t AS user_id,
                a.updated_at,
                a.revenue,
                a.payment_plan_id,
                CASE
                    WHEN legal_club_id > 0 THEN legal_club_id
                    ELSE club_id
                END AS main_club_id
            FROM all_transactions_users a
            LEFT JOIN extraction_plus_prev_next t ON a.user_id_t = t.user_id AND a.user_payment_plan_id = t.id
            WHERE a.revenue IS NOT NULL
        )
        SELECT 
            toStartOfMonth(f.updated_at) AS date,
            f.payment_plan_id,
            pp.name AS payment_name,
            f.main_club_id AS club_id,
            c.name AS club_name,
            c.city_id,
            c.club_type,
            c.open_date,
            sum(f.revenue) AS total_cost
        FROM final_table f 
        LEFT JOIN ddxfitness_prod_v2.pg_clubs c ON f.main_club_id = c.id
        LEFT JOIN ddxfitness_prod_v2.pg_payment_plans pp ON f.payment_plan_id = pp.id
        GROUP BY
            date, f.payment_plan_id, pp.name,
            f.main_club_id, c.name, c.city_id, c.club_type, c.open_date
        """

    load_monthly_data()
