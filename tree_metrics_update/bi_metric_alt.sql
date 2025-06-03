TRUNCATE TABLE ddxfitness_prod_v2.bi_metric_alt;

INSERT INTO ddxfitness_prod_v2.bi_metric_alt
WITH
    main_table AS (
        SELECT
            user_id,
            inflow_club,
            inflow_club_name,
            start_date,
            end_date,
            user_status,
            leadInFrame(start_date) OVER (PARTITION BY user_id ORDER BY start_date ASC) AS next_start,
            leadInFrame(end_date) OVER (PARTITION BY user_id ORDER BY end_date ASC) AS next_end,
            row_number() OVER (PARTITION BY user_id ORDER BY start_date ASC) AS rnk
        FROM ddxfitness_prod_v2.inflow_and_outflow_new
        ORDER BY start_date ASC
    ),
    diff_date_and_lifetime AS (
        SELECT
            user_id,
            inflow_club,
            inflow_club_name,
            start_date,
            end_date,
            user_status,
            multiIf(
                next_start != '1970-01-01', dateDiff('day', end_date, next_start),
                (next_start = '1970-01-01') AND (end_date IS NOT NULL), 999,
                NULL
            ) AS diff_date_day,
            multiIf(end_date IS NOT NULL, dateDiff('day', start_date, end_date), NULL) AS life_time
        FROM main_table
        WHERE end_date IS NOT NULL
    ),
    avg_life_time AS (
        SELECT
            user_id,
            inflow_club,
            inflow_club_name,
            start_date,
            end_date,
            user_status,
            diff_date_day,
            life_time,
            multiIf(diff_date_day > 90, 1,
                diff_date_day <= 90, 1 / countIf(user_id, diff_date_day <= 90) OVER (PARTITION BY user_id),
                (diff_date_day IS NULL) AND (end_date IS NOT NULL), 1,
                NULL
            ) AS cnt_life_time,
            multiIf(diff_date_day > 90, 1, NULL) AS flaq,
            sum(coalesce(flaq, 0)) OVER (PARTITION BY user_id) AS sum_flaq,
            count(user_id) OVER (PARTITION BY user_id) AS cnt_lt
        FROM diff_date_and_lifetime
    ),
    alt_by_clubs_month AS (
        SELECT
            r.user_id,
            q.club_life_time,
            q.inflow_club_name,
            multiIf(
                cnt_lt > 1 AND sum_flaq = 0, sum(life_time) / sum(cnt_life_time),
                cnt_lt > 1 AND sum_flaq > 1, sum(life_time) / sum(flaq),
                sum_flaq = 1, sum(life_time) / sum(cnt_life_time),
                sum(life_time) / sum(cnt_life_time)
            ) AS alt,
            max(end_date) AS date_life_time
        FROM avg_life_time r
        LEFT JOIN (
            SELECT
                user_id,
                inflow_club AS club_life_time,
                inflow_club_name,
                sum(life_time) AS all_club_life_time,
                row_number() OVER (PARTITION BY user_id ORDER BY sum(life_time) DESC) AS rnk_club_life_time
            FROM avg_life_time
            GROUP BY user_id, inflow_club, inflow_club_name
            HAVING all_club_life_time IS NOT NULL
        ) AS q ON r.user_id = q.user_id AND q.rnk_club_life_time = 1
        GROUP BY r.user_id, q.club_life_time, q.inflow_club_name, cnt_lt, sum_flaq
        HAVING alt > 0
    ),
    final_alt AS (
        SELECT
            toStartOfMonth(date_life_time) AS date,
            club_life_time AS club_id,
            inflow_club_name AS club_name,
            round(avg(alt), 0) AS alt
        FROM alt_by_clubs_month
        GROUP BY 1, 2, 3
    )
SELECT
    date,
    club_id,
    club_name,
    alt
FROM final_alt;
