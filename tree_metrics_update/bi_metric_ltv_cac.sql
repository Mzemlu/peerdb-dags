TRUNCATE TABLE ddxfitness_prod_v2.bi_metric_ltv_cac;

INSERT INTO ddxfitness_prod_v2.bi_metric_ltv_cac
WITH
    calendar AS (
        SELECT
            toStartOfMonth(d.day_id_day) AS date_month,
            s.id AS club_id,
            s.name AS club_name
        FROM ddxfitness_prod_v2.d_day AS d,
             ddxfitness_prod_v2.pg_clubs AS s
        WHERE d.day_id_day <= today()
        GROUP BY date_month, s.id, s.name
    ),
    is_inflow_month AS (
        SELECT
            toStartOfMonth(start_date) AS date_month,
            inflow_club,
            inflow_club_name AS club_name,
            sum(is_inflow) AS is_inflow
        FROM ddxfitness_prod_v2.inflow_and_outflow_new
        GROUP BY date_month, inflow_club, club_name
    ),
    outflow_table AS (
        SELECT
            toStartOfMonth(end_date) AS month_date,
            club_id,
            club_name,
            sum(is_outflow) AS outflow
        FROM ddxfitness_prod_v2.active_users_by_clubs
        WHERE is_outflow = 1
        GROUP BY month_date, club_id, club_name
    ),
    inflow_table AS (
        SELECT
            toStartOfMonth(start_date) AS month_date,
            club_id,
            club_name,
            sum(is_inflow) AS inflow
        FROM ddxfitness_prod_v2.active_users_by_clubs
        WHERE is_inflow = 1
        GROUP BY month_date, club_id, club_name
    ),
    all_table AS (
        SELECT
            coalesce(o.month_date, i.month_date) AS month_date,
            coalesce(o.club_id, i.club_id) AS club_id,
            coalesce(o.club_name, i.club_name) AS club_name,
            coalesce(o.outflow, 0) AS outflow,
            coalesce(i.inflow, 0) AS inflow
        FROM outflow_table AS o
        FULL OUTER JOIN inflow_table AS i
            ON o.month_date = i.month_date AND o.club_id = i.club_id
    ),
    grouping_inflow_outflow AS (
        SELECT
            month_date,
            club_id,
            club_name,
            sum(outflow) AS outflow,
            sum(inflow) AS inflow
        FROM all_table
        GROUP BY month_date, club_id, club_name
    ),
    cac_calculation AS (
        SELECT
            r.date_month,
            r.club_id,
            r.club_name,
            s.open_date,
            abs(ae.acquisition_cost) AS acquisition_cost,
            ae.club_ebitda,
            h.is_inflow,
            coalesce(
                sum(w.inflow) OVER (PARTITION BY w.club_id ORDER BY w.month_date ASC), 0
            ) - coalesce(
                sum(w.outflow) OVER (PARTITION BY w.club_id ORDER BY w.month_date ASC), 0
            ) AS active_subscribes,
            abs(ae.acquisition_cost) / nullIf(h.is_inflow, 0) AS cac
        FROM calendar r
        LEFT JOIN grouping_inflow_outflow w
            ON r.date_month = w.month_date AND r.club_id = w.club_id
        LEFT JOIN ddxfitness_prod_v2.all_expenses ae
            ON r.date_month = CAST(ae.date_month, 'date') AND r.club_id = ae.club_id
        LEFT JOIN ddxfitness_prod_v2.pg_clubs s
            ON r.club_id = s.id
        LEFT JOIN is_inflow_month h
            ON r.date_month = h.date_month AND r.club_id = h.inflow_club
    )
SELECT
    date_month AS date,
    club_id,
    club_name,
    open_date,
    acquisition_cost,
    club_ebitda,
    is_inflow,
    active_subscribes,
    cac,
    club_ebitda / nullIf(active_subscribes, 0) AS ltv,
    (club_ebitda / active_subscribes) / nullIf(cac, 0) AS ratio_ltv_cac
FROM cac_calculation
WHERE date_month >= toStartOfMonth(open_date);
