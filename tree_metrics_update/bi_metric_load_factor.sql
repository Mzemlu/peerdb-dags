TRUNCATE TABLE ddxfitness_prod_v2.bi_metric_load_factor;

INSERT INTO ddxfitness_prod_v2.bi_metric_load_factor
WITH
    calendar AS (
        SELECT *
        FROM (
            SELECT toStartOfMonth(day_id_day) AS day_id_day
            FROM ddxfitness_prod_v2.d_day
            WHERE day_id_day <= today() AND day_id_day >= '2018-03-01'
            GROUP BY 1
        ) t1
        CROSS JOIN (
            SELECT
                id,
                name AS club_name
            FROM ddxfitness_prod_v2.pg_clubs
            WHERE open_date <= today()
        ) t2
    ),
    inflow_without_migration AS (
        SELECT
            toStartOfMonth(start_date) AS date,
            inflow_club_name AS club_name,
            sum(is_inflow) AS inflow
        FROM ddxfitness_prod_v2.inflow_and_outflow_new
        WHERE is_inflow = 1
        GROUP BY 1, 2
    ),
    outflow_without_migration AS (
        SELECT
            toStartOfMonth(end_date) AS date,
            outflow_club_name AS club_name,
            sum(is_outflow) AS outflow
        FROM ddxfitness_prod_v2.inflow_and_outflow_new
        WHERE is_outflow = 1
        GROUP BY 1, 2
    ),
    grouped_inflow_outflow_without_migration AS (
        SELECT
            day_id_day AS date,
            id AS club_id,
            t1.club_name AS club_name,
            t2.inflow,
            t3.outflow
        FROM calendar t1
        LEFT JOIN inflow_without_migration t2 ON (t1.day_id_day = t2.date AND t1.club_name = t2.club_name)
        LEFT JOIN outflow_without_migration t3 ON (t1.day_id_day = t3.date AND t1.club_name = t3.club_name)
    ),
    inflow_outflow_with_migration AS (
        SELECT
            coalesce(i.month_date, o.month_date) AS month_date,
            coalesce(i.club_id, o.club_id) AS club_id,
            coalesce(i.club_name, o.club_name) AS club_name,
            coalesce(o.outflow, 0) AS outflow,
            coalesce(i.inflow, 0) AS inflow
        FROM (
            SELECT
                toStartOfMonth(start_date) AS month_date,
                club_id,
                club_name,
                sum(is_inflow) AS inflow
            FROM ddxfitness_prod_v2.active_users_by_clubs
            WHERE is_inflow = 1
            GROUP BY 1, 2, 3
        ) i
        FULL OUTER JOIN (
            SELECT
                toStartOfMonth(end_date) AS month_date,
                club_id,
                club_name,
                sum(is_outflow) AS outflow
            FROM ddxfitness_prod_v2.active_users_by_clubs
            WHERE is_outflow = 1
            GROUP BY 1, 2, 3
        ) o ON i.month_date = o.month_date AND i.club_id = o.club_id AND i.club_name = o.club_name
    ),
    grouped_inflow_outflow_with_migration AS (
        SELECT
            month_date,
            club_id,
            club_name,
            sum(outflow) AS outflow_with_migration,
            sum(inflow) AS inflow_with_migration
        FROM inflow_outflow_with_migration
        GROUP BY 1, 2, 3
    ),
    area_meters AS (
        SELECT
            w.month_date,
            w.club_id,
            w.club_name,
            w.outflow_with_migration,
            w.inflow_with_migration,
            coalesce(s.rounded_area, s.area_in_square_meters) AS area_in_square_meters,
            coalesce(
                sum(w.inflow_with_migration) OVER (PARTITION BY w.club_id ORDER BY w.month_date ASC), 0
            ) - coalesce(
                sum(w.outflow_with_migration) OVER (PARTITION BY w.club_id ORDER BY w.month_date ASC), 0
            ) AS active_subscribes
        FROM grouped_inflow_outflow_with_migration w
        LEFT JOIN ddxfitness_prod_v2.pg_clubs s ON w.club_id = s.id
    ),
    final_load_factor AS (
        SELECT
            t1.date,
            t1.club_id,
            t1.club_name,
            t1.outflow,
            t1.inflow,
            t2.outflow_with_migration,
            t2.inflow_with_migration,
            t2.active_subscribes,
            t2.area_in_square_meters,
            round(t2.active_subscribes / t2.area_in_square_meters, 2) AS load_factor
        FROM grouped_inflow_outflow_without_migration t1
        LEFT JOIN area_meters t2 ON t1.date = t2.month_date AND t1.club_name = t2.club_name
    )
SELECT *
FROM final_load_factor;
