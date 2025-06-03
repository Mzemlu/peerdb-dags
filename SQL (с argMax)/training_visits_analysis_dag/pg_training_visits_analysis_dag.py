from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="pg_training_visits_analysis_dag",
    schedule_interval="0 4 * * *",
    default_args=default_args,
    catchup=False,
    tags=["training_visits", "daily"]
) as dag:

    @task
    def create_training_visits_table():
        ch = ClickHouseHook(
            clickhouse_conn_id="clickhouse_peerdb_prod",
            database="ddxfitness_prod_v2"
        )

        ch.execute("DROP TABLE IF EXISTS ddxfitness_prod_v2.training_visits_analysis")

        create_sql = """
        CREATE TABLE ddxfitness_prod_v2.training_visits_analysis
        ENGINE = MergeTree
        ORDER BY (booking_id, user_id)
        AS
        WITH dedup_user_payment_plans AS (
            SELECT
                id,
                argMax(user_id, _peerdb_version) AS user_id,
                argMax(signed_date, _peerdb_version) AS signed_date,
                argMax(end_date, _peerdb_version) AS end_date,
                argMax(status, _peerdb_version) AS status,
                argMax(payment_plan_id, _peerdb_version) AS payment_plan_id
            FROM ddxfitness_prod_v2.pg_user_payment_plans
            GROUP BY id
        ),
        list AS (
            SELECT
                CAST(vh.id AS Int64) AS id,
                CAST(vh.club_id AS Int64) AS club_id,
                vh.club_name,
                CAST(vh.user_id AS Int64) AS user_id,
                vh.in_date,
                vh.out_date,
                vh.event_type
            FROM ddxfitness_prod_v2.club_data_visits AS vh
            INNER JOIN (
                SELECT
                    CAST(user_id AS UInt64) AS user_id,
                    signed_date + INTERVAL 6 HOUR AS signed_date,
                    multiIf(
                        end_date IS NULL AND status IN ('Current', 'Freezed', 'NotStarted', 'PaymentPending'),
                        toDateTime('2099-12-31'),
                        end_date + INTERVAL 6 HOUR
                    ) AS end_date
                FROM dedup_user_payment_plans
                WHERE payment_plan_id NOT IN (1, 111, 152, 161)
                  AND status != 'Deleted'
            ) AS upp ON vh.user_id = upp.user_id
            WHERE toDate(vh.in_date) >= toDate('2023-01-01')
              AND vh.in_date BETWEEN upp.signed_date AND upp.end_date
            GROUP BY id, club_id, club_name, user_id, in_date, out_date, event_type
        ),
        visits AS (
            SELECT
                id,
                club_id,
                club_name,
                user_id,
                in_date,
                out_date,
                event_type,
                multiIf(
                    toDate(in_date) = toDate(out_date - INTERVAL 3 HOUR), 1, 0
                ) AS is_visit
            FROM list
            WHERE toDate(in_date) = toDate(out_date - INTERVAL 3 HOUR)
        ),
        final_table AS (
            SELECT
                t1.booking_id,
                t1.timetable_id,
                t1.count_seats,
                t1.club_zone,
                t1.employee_id,
                t1.group_training_id,
                t1.group_name,
                t1.category_name,
                t1.club_name,
                t1.start_time,
                t1.end_time,
                t2.in_date,
                t2.out_date,
                COALESCE(t1.last_name, t1.name) AS last_name,
                t1.name,
                t1.user_id,
                t1.phone,
                t1.booking_status,
                t1.booking_date,
                t1.updating_date,
                multiIf(
                    t1.start_time >= t2.in_date + INTERVAL 1 MINUTE AND
                    t1.end_time <= t2.out_date + INTERVAL 1 MINUTE,
                    'Yes', 'No'
                ) AS is_visit
            FROM ddxfitness_prod_v2.bi_training_list AS t1
            LEFT JOIN visits AS t2
              ON t1.user_id = t2.user_id
             AND toDate(t1.start_time) = toDate(t2.in_date)
            WHERE t1.start_time >= t2.in_date + INTERVAL 1 MINUTE
              AND t1.end_time <= t2.out_date + INTERVAL 1 MINUTE
        ),
        booking_result AS (
            SELECT
                t1.*,
                t2.is_visit,
                multiIf(is_visit = '' AND booking_status = 'booked', 1, 0) AS is_visit_no,
                multiIf(booking_status = 'wait_list', 1, 0) AS is_visit_waiting,
                multiIf(booking_status = 'cancelled', 1, 0) AS is_visit_cancel,
                multiIf(is_visit = 'Yes' AND booking_status = 'booked', 1, 0) AS is_visit_yes
            FROM ddxfitness_prod_v2.bi_training_list AS t1
            LEFT JOIN final_table AS t2 ON t1.booking_id = t2.booking_id
        )
        SELECT
            *,
            multiIf(
                is_visit = 'Yes' AND booking_status = 'booked', 'Yes',
                booking_status = 'waiting_list', 'waiting_list',
                booking_status = 'cancelled', 'cancelled',
                'No'
            ) AS is_visit_calc
        FROM booking_result;
        """

        ch.execute(create_sql)

    create_training_visits_table()
