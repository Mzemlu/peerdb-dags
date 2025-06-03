from airflow import DAG
from airflow.decorators import task
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

INCREMENT_DAYS = 2

with DAG(
    dag_id="pg_club_in_out_incremental_dag",
    schedule_interval="0 3 * * *",
    default_args=default_args,
    catchup=False,
    tags=["club_in_out", "daily", "incremental"]
) as dag:

    @task
    def create_table_if_not_exists():
        ch = ClickHouseHook(
            clickhouse_conn_id="ddxfitness_prod_v2",
            database="clickhouse_peerdb_prod"
        )
        create_sql = """
        CREATE TABLE IF NOT EXISTS ddxfitness_prod_v2.club_data_raw
        (
            id              UInt64,
            user_id         UInt64,
            user_name       String,
            last_name       String,
            email           String,
            sex             String,
            birthday        DateTime,
            club_id         UInt64,
            club_name       String,
            event_date      DateTime,
            event_type      String,
            next_event_date DateTime,
            prev_event_date DateTime,
            next_event      String,
            prev_event      String
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (user_id, event_date, id)
        """
        ch.execute(create_sql)

    @task
    def incremental_load_lead_lag():
        ch = ClickHouseHook(
            clickhouse_conn_id="ddxfitness_prod_v2",
            database="clickhouse_peerdb_prod"
        )
        today = datetime.now().date()
        date_from = today - timedelta(days=INCREMENT_DAYS)
        date_to = today

        delete_sql = f"""
        ALTER TABLE ddxfitness_prod_v2.club_data_raw
        DELETE WHERE toDate(event_date) BETWEEN '{date_from}' AND '{date_to}'
        """
        ch.execute(delete_sql)

        insert_sql = f"""
        INSERT INTO ddxfitness_prod_v2.club_data_raw
        SELECT
            vh.id,
            vh.user_id,
            u.name,
            COALESCE(u.last_name, ''),
            u.email,
            COALESCE(u.sex, ''),
            multiIf(
                u.birthday IS NULL, NULL,
                CAST(u.birthday, 'UInt16') >= 39969,
                CAST(CAST(u.birthday, 'UInt16') - 65536, 'DateTime'),
                u.birthday
            ),
            vh.club_id,
            c.name,
            toTimezone(vh.club_event_date, 'Europe/Moscow'),
            vh.event_type,
            leadInFrame(vh.club_event_date, 1) OVER (
                PARTITION BY vh.user_id ORDER BY vh.club_event_date ASC
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
            ),
            lagInFrame(vh.club_event_date, 1) OVER (
                PARTITION BY vh.user_id ORDER BY vh.club_event_date ASC
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            ),
            leadInFrame(vh.event_type, 1) OVER (
                PARTITION BY vh.user_id ORDER BY vh.club_event_date ASC
                ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
            ),
            lagInFrame(vh.event_type, 1) OVER (
                PARTITION BY vh.user_id ORDER BY vh.club_event_date ASC
                ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
            )
        FROM ddxfitness_prod_v2.pg_visits_histories AS vh
        LEFT JOIN ddxfitness_prod_v2.pg_users AS u ON vh.user_id = u.id
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c ON vh.club_id = c.id
        WHERE vh.event_type IN ('club_in', 'club_out')
          AND toDate(vh.club_event_date) BETWEEN '{date_from}' AND '{date_to}'
        """
        ch.execute(insert_sql)

    @task
    def create_club_in_table():
        ch = ClickHouseHook(clickhouse_conn_id="ddxfitness_prod_v2", database="clickhouse_peerdb_prod")
        ch.execute("DROP TABLE IF EXISTS ddxfitness_prod_v2.club_data_in")
        ch.execute("""
        CREATE TABLE ddxfitness_prod_v2.club_data_in
        ENGINE = MergeTree
        ORDER BY (user_id, id)
        AS
        SELECT
            id, user_id, user_name, last_name, email, sex, birthday,
            club_id, club_name,
            event_date AS in_date,
            event_type,
            CASE 
                WHEN (
                    (next_event_date IS NULL OR next_event_date < toDateTime('1971-01-01'))
                    AND event_type = 'club_in' 
                    AND next_event = 'club_out'
                )
                OR (event_type = 'club_in' AND dateDiff('day', event_date, next_event_date) >= 1)
                OR (event_type = 'club_in' AND next_event = 'club_in')
                THEN event_date + INTERVAL 90 MINUTE
                ELSE next_event_date
            END AS out_date
        FROM ddxfitness_prod_v2.club_data_raw
        WHERE event_type = 'club_in'
          AND (
              next_event = 'club_out'
              OR next_event_date < toDateTime('1971-01-01')  
              OR dateDiff('day', event_date, next_event_date) >= 1
              OR next_event = 'club_in'
          )
        """)

    @task
    def create_club_out_table():
        ch = ClickHouseHook(clickhouse_conn_id="ddxfitness_prod_v2", database="clickhouse_peerdb_prod")
        ch.execute("DROP TABLE IF EXISTS ddxfitness_prod_v2.club_data_out")
        ch.execute("""
        CREATE TABLE ddxfitness_prod_v2.club_data_out
        ENGINE = MergeTree
        ORDER BY (user_id, id)
        AS
        SELECT
            id, user_id, user_name, last_name, email, sex, birthday,
            club_id, club_name,
            CASE 
                WHEN (
                    (next_event_date IS NULL OR next_event_date < toDateTime('1971-01-01'))
                    AND event_type = 'club_out'
                    AND prev_event = 'club_in'
                )
                OR (event_type = 'club_out' AND dateDiff('day', event_date, prev_event_date) >= 1)
                OR (event_type = 'club_out' AND prev_event = 'club_out')
                THEN event_date - INTERVAL 90 MINUTE
                ELSE prev_event_date
            END AS in_date,
            event_type,
            event_date AS out_date
        FROM ddxfitness_prod_v2.club_data_raw
        WHERE event_type = 'club_out'
          AND (
              prev_event = 'club_in'
              OR prev_event_date < toDateTime('1971-01-01')  
              OR dateDiff('day', event_date, prev_event_date) >= 1
              OR prev_event = 'club_out'
          )
        """)

    @task
    def create_club_visits_table():
        ch = ClickHouseHook(clickhouse_conn_id="ddxfitness_prod_v2", database="clickhouse_peerdb_prod")
        ch.execute("DROP TABLE IF EXISTS ddxfitness_prod_v2.club_data_visits")
        ch.execute("""
        CREATE TABLE ddxfitness_prod_v2.club_data_visits
        ENGINE = MergeTree
        ORDER BY (user_id, in_date)
        AS
        SELECT * FROM ddxfitness_prod_v2.club_data_in
        UNION ALL
        SELECT * FROM ddxfitness_prod_v2.club_data_out
        """)

    t_create_raw = create_table_if_not_exists()
    t_increment = incremental_load_lead_lag()
    t_in = create_club_in_table()
    t_out = create_club_out_table()
    t_visits = create_club_visits_table()

    t_create_raw >> t_increment >> [t_in, t_out] >> t_visits
