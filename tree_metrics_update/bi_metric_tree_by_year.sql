TRUNCATE TABLE ddxfitness_prod_v2.bi_metric_tree_by_year;

INSERT INTO ddxfitness_prod_v2.bi_metric_tree_by_year
WITH
    calendar AS (
        SELECT
            toStartOfMonth(day_id_day) AS date,
            name AS club_name
        FROM ddxfitness_prod_v2.d_day, ddxfitness_prod_v2.pg_clubs
        WHERE day_id_day <= today()
        GROUP BY 1, 2
    ),
    main AS (
        SELECT
            cal.date,
            toString(toYear(cal.date)) AS year,
            t2.club_id,
            cal.club_name,
            c.club_type,
            multiIf(c.id IN (5, 9, 14, 15, 19, 25, 26, 31, 34, 40, 42, 47, 50), '1',
                    c.id IN (2, 3, 4, 7, 8, 11, 13, 17, 21, 24, 27, 28, 30, 33, 35, 36, 37, 38, 39, 41, 44, 46, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63), '2',
                    '3') AS tir,
            multiIf(
                dateDiff('month', c.open_date, now()) <= 12, 'молодые',
                dateDiff('month', c.open_date, now()) <= 24, 'юниоры',
                'зрелые') AS age,
            t2.alt,
            t2.outflow,
            t2.inflow,
            t2.area_in_square_meters,
            t2.load_factor,
            t2.revenue,
            t2.acquisition_cost,
            t2.club_ebitda,
            t2.active_subscribers,
            t2.cac,
            t2.ltv,
            t2.arpu,
            t2.nsm_metric
        FROM calendar AS cal
        LEFT JOIN ddxfitness_prod_v2.bi_metric_tree_by_month AS t2
            ON cal.date = t2.date AND cal.club_name = t2.club_name
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c
            ON cal.club_name = c.name
    ),
    main2 AS (
        SELECT
            cal.date AS date_2,
            toString(toYear(cal.date)) AS year_2,
            t2.club_id AS club_id_2,
            cal.club_name AS club_name_2,
            c.club_type AS club_type_2,
            multiIf(c.id IN (5, 9, 14, 15, 19, 25, 26, 31, 34, 40, 42, 47, 50), '1',
                    c.id IN (2, 3, 4, 7, 8, 11, 13, 17, 21, 24, 27, 28, 30, 33, 35, 36, 37, 38, 39, 41, 44, 46, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63), '2',
                    '3') AS tir_2,
            multiIf(
                dateDiff('month', c.open_date, now()) <= 12, 'молодые',
                dateDiff('month', c.open_date, now()) <= 24, 'юниоры',
                'зрелые') AS age_2,
            t2.alt AS alt_2,
            t2.outflow AS outflow_2,
            t2.inflow AS inflow_2,
            t2.area_in_square_meters AS area_in_square_meters_2,
            t2.load_factor AS load_factor_2,
            t2.revenue AS revenue_2,
            t2.acquisition_cost AS acquisition_cost_2,
            t2.club_ebitda AS club_ebitda_2,
            t2.active_subscribers AS active_subscribers_2,
            t2.cac AS cac_2,
            t2.ltv AS ltv_2,
            t2.arpu AS arpu_2,
            t2.nsm_metric AS nsm_metric_2
        FROM calendar AS cal
        LEFT JOIN ddxfitness_prod_v2.bi_metric_tree_by_month AS t2
            ON cal.date = t2.date AND cal.club_name = t2.club_name
        LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c
            ON cal.club_name = c.name
    )
SELECT
    multiIf(main.date = '1970-01-01', main2.date_2, main.date) AS date,
    main.year,
    main.club_id,
    main.club_name,
    main.club_type,
    main.tir,
    main.age,
    main.alt,
    main.outflow,
    main.inflow,
    main.area_in_square_meters,
    main.load_factor,
    main.revenue,
    main.acquisition_cost,
    main.club_ebitda,
    main.active_subscribers,
    main.cac,
    main.ltv,
    main.arpu,
    main.nsm_metric,
    main2.year_2,
    main2.club_id_2,
    main2.club_name_2,
    main2.club_type_2,
    main2.tir_2,
    main2.age_2,
    main2.alt_2,
    main2.outflow_2,
    main2.inflow_2,
    main2.area_in_square_meters_2,
    main2.load_factor_2,
    main2.revenue_2,
    main2.acquisition_cost_2,
    main2.club_ebitda_2,
    main2.active_subscribers_2,
    main2.cac_2,
    main2.ltv_2,
    main2.arpu_2,
    main2.nsm_metric_2
FROM main
LEFT JOIN main2
    ON toMonth(main.date) = toMonth(main2.date_2)
SETTINGS join_use_nulls = 1;
