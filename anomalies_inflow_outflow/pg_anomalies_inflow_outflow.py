from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from plugins.operators.pachca.pachca_notify_operator import PachcaNotifyOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pg_anomalies_inflow_outflow',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=["anomalies", "analytics"]
) as dag:

    def check_anomalies(**kwargs):
        clickhouse_hook = ClickHouseHook(
            clickhouse_conn_id='clickhouse_peerdb_prod',
            database='ddxfitness_prod_v2'
        )

        inflow_query = """
        WITH daily_inflow AS (
            SELECT
                toDate(start_date) AS inflow_date,
                COUNT(user_id) AS new_subscribers
            FROM ddxfitness_prod_v2.inflow_and_outflow_new
            WHERE is_inflow = 1
            GROUP BY inflow_date
        ),
        moving_average_inflow AS (
            SELECT
                inflow_date,
                new_subscribers,
                AVG(new_subscribers) OVER (ORDER BY inflow_date ASC ROWS BETWEEN 179 PRECEDING AND CURRENT ROW) AS avg_6_months,
                arrayJoin(
                    [new_subscribers] + arraySlice(
                        groupArray(new_subscribers) OVER (ORDER BY inflow_date ASC),
                        -1
                    )
                ) AS previous_day_subscribers
            FROM daily_inflow
        ),
        inflow_anomalies AS (
            SELECT
                inflow_date,
                new_subscribers,
                avg_6_months,
                previous_day_subscribers,
                CASE
                    WHEN new_subscribers > avg_6_months * 1.8 THEN 1
                    WHEN new_subscribers > previous_day_subscribers * 2.5 THEN 1
                    ELSE 0
                END AS is_anomaly
            FROM moving_average_inflow
        )
        SELECT count(*) AS cnt
        FROM inflow_anomalies
        WHERE is_anomaly = 1
          AND inflow_date = yesterday()
        """

        outflow_query = """
        WITH daily_outflow AS (
            SELECT
                toDate(end_date) AS outflow_date,
                COUNT(user_id) AS churned_subscribers
            FROM ddxfitness_prod_v2.inflow_and_outflow_new
            WHERE is_outflow = 1
            GROUP BY outflow_date
        ),
        monthly_active AS (
            SELECT
                date,
                SUM(active_subscribers) AS active_subs
            FROM ddxfitness_prod_v2.bi_metric_tree_by_month
            GROUP BY date
        ),
        daily_with_month AS (
            SELECT
                outflow_date,
                churned_subscribers,
                date_trunc('month', outflow_date) AS month_start
            FROM daily_outflow
        ),
        joined AS (
            SELECT
                dwm.outflow_date,
                dwm.churned_subscribers,
                ma.active_subs,
                dateDiff('day', month_start, addMonths(month_start, 1)) AS days_in_month
            FROM daily_with_month dwm
            LEFT JOIN monthly_active ma ON dwm.month_start = ma.date
        ),
        calc_rate AS (
            SELECT
                outflow_date,
                churned_subscribers,
                active_subs,
                days_in_month,
                churned_subscribers / NULLIF(active_subs / days_in_month, 0) AS daily_churn_rate
            FROM joined
        ),
        anomaly_stats AS (
            SELECT
                outflow_date,
                churned_subscribers,
                daily_churn_rate,
                quantileExact(0.5)(daily_churn_rate) OVER (
                    ORDER BY outflow_date ASC ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
                ) AS median_6months,
                (
                    quantileExact(0.75)(daily_churn_rate) OVER (
                        ORDER BY outflow_date ASC ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
                    ) -
                    quantileExact(0.25)(daily_churn_rate) OVER (
                        ORDER BY outflow_date ASC ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
                    )
                ) AS iqr_6months,
                arrayJoin(
                    [ifNull(daily_churn_rate, 0.0)]
                    + arraySlice(
                        groupArray(ifNull(daily_churn_rate, 0.0)) OVER (ORDER BY outflow_date ASC),
                        -1
                    )
                ) AS prev_day_rate
            FROM calc_rate
        ),
        outflow_anomalies AS (
            SELECT
                outflow_date,
                churned_subscribers,
                CASE
                    WHEN daily_churn_rate > median_6months + 1.5 * iqr_6months THEN 1
                    WHEN prev_day_rate > 0 AND daily_churn_rate > prev_day_rate * 2 THEN 1
                    ELSE 0
                END AS is_anomaly
            FROM anomaly_stats
        )
        SELECT count(*) AS cnt
        FROM outflow_anomalies
        WHERE is_anomaly = 1
          AND outflow_date = yesterday()
        """

        inflow_cnt = clickhouse_hook.execute(inflow_query)[0][0]
        outflow_cnt = clickhouse_hook.execute(outflow_query)[0][0]

        has_anomalies = (inflow_cnt > 0) or (outflow_cnt > 0)
        print(f"[Check anomalies] inflow_cnt={inflow_cnt}, outflow_cnt={outflow_cnt}, has_anomalies={has_anomalies}")
        return has_anomalies

    check_anomalies_task = PythonOperator(
        task_id='check_anomalies',
        python_callable=check_anomalies
    )

    short_circuit_if_no_anomaly = ShortCircuitOperator(
        task_id='short_circuit_if_no_anomaly',
        python_callable=lambda ti: ti.xcom_pull(task_ids='check_anomalies')
    )

    send_alert = PachcaNotifyOperator(
        task_id='send_alert',
        access_token='X9zEI3fOZIT66xR2B1D02o2RW1vC4SsTDxYBiY3TGm0',
        chat_id='20629635',
        message=(
            "Обнаружены аномалии в притоке/оттоке за вчерашний день! "
            "Детали смотрите тут: https://bi.ddxfitness.ru/superset/dashboard/488"
        )
    )

    check_anomalies_task >> short_circuit_if_no_anomaly >> send_alert

if __name__ == "__main__":
    dag.test()
