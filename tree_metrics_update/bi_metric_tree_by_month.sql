TRUNCATE TABLE ddxfitness_prod_v2.bi_metric_tree_by_month;

INSERT INTO ddxfitness_prod_v2.bi_metric_tree_by_month
WITH
    club_list AS (
        SELECT
            date,
            club_name,
            club_id
        FROM (
            SELECT toStartOfMonth(day_id_day) AS date
            FROM ddxfitness_prod_v2.d_day
            WHERE day_id_day <= today()
            GROUP BY 1
        ) t1
        CROSS JOIN (
            SELECT
                id AS club_id,
                name AS club_name
            FROM ddxfitness_prod_v2.pg_clubs
        ) t2
    ),
    main AS (
        SELECT
            left(toString(multiIf(
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(11)) AND cl.date <= today(), toStartOfMonth(today()),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(23)) AND cl.date <= (today() - toIntervalMonth(12)), toStartOfMonth(today() - toIntervalMonth(12)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(35)) AND cl.date <= (today() - toIntervalMonth(24)), toStartOfMonth(today() - toIntervalMonth(24)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(47)) AND cl.date <= (today() - toIntervalMonth(36)), toStartOfMonth(today() - toIntervalMonth(36)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(59)) AND cl.date <= (today() - toIntervalMonth(48)), toStartOfMonth(today() - toIntervalMonth(48)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(71)) AND cl.date <= (today() - toIntervalMonth(60)), toStartOfMonth(today() - toIntervalMonth(60)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(83)) AND cl.date <= (today() - toIntervalMonth(72)), toStartOfMonth(today() - toIntervalMonth(72)),
                cl.date >= (toStartOfMonth(today()) - toIntervalMonth(95)) AND cl.date <= (today() - toIntervalMonth(84)), toStartOfMonth(today() - toIntervalMonth(84)),
                toDate('2018-01-01')
            )), 4) AS year,
            cl.date,
            cl.club_id,
            cl.club_name,
            alt.alt,
            lf.outflow,
            lf.inflow,
            lf.area_in_square_meters,
            lf.load_factor,
            r.revenue,
            ltv.acquisition_cost,
            ltv.club_ebitda,
            ltv.active_subscribers,
            ltv.cac,
            ltv.ltv,
            r.revenue / nullIf(ltv.active_subscribers, 0) AS arpu,
            ((lf.load_factor * alt.alt) / 30) * (r.revenue / nullIf(ltv.active_subscribers, 0)) AS nsm_metric
        FROM club_list cl
        LEFT JOIN ddxfitness_prod_v2.bi_metric_alt alt
            ON cl.club_name = alt.club_name AND cl.date = alt.date
        LEFT JOIN ddxfitness_prod_v2.bi_metric_load_factor lf
            ON cl.date = lf.date AND cl.club_name = lf.club_name
        LEFT JOIN (
            SELECT
                toStartOfMonth(date) AS date,
                club_name,
                sum(total_cost) AS revenue
            FROM ddxfitness_prod_v2.revenue
            GROUP BY 1, 2
        ) r ON cl.date = r.date AND cl.club_name = r.club_name
        LEFT JOIN ddxfitness_prod_v2.bi_metric_ltv_cac ltv
            ON cl.date = ltv.date AND cl.club_name = ltv.club_name
    )
SELECT *
FROM main
SETTINGS join_use_nulls = 1;
