TRUNCATE TABLE ddxfitness_prod_v2.bi_marketing_all_users;

INSERT INTO ddxfitness_prod_v2.bi_marketing_all_users
WITH extraction AS (
    SELECT
        upp.discount_code_id AS discount_code_id,
        upp.user_id AS id,
        upp.id AS sub_id,
        CASE 
            WHEN dipp.id > 0 THEN 'main'
            WHEN lower(pp.name) LIKE '%бартер%' THEN 'barter'
            WHEN lower(pp.name) LIKE '%trial%' THEN 'trial'
            ELSE 'sub'
        END AS subscription_type, 
        upp.payment_plan_id,
        upp.discount_id,
        upp.signed_date + toIntervalHour(6) AS signed_date,
        upp.start_date + toIntervalHour(6) AS start_date,
        upp.end_date + toIntervalHour(6) AS end_date,
        upp.club_id,
        upp.status,
        multiIf(upp.card_token_id IS NULL, false, true) AS card_token_id,
        coalesce(upp.cancel_reason, '-') AS cancel_reason
    FROM ddxfitness_prod_v2.pg_user_payment_plans AS upp
    LEFT JOIN ddxfitness_prod_v2.d_inflow_payment_plans AS dipp ON dipp.id = upp.payment_plan_id
    LEFT JOIN ddxfitness_prod_v2.pg_payment_plans AS pp ON pp.id = upp.payment_plan_id
    WHERE coalesce(upp.cancel_reason, '-') != 'ExcludeFromReport'
      AND upp.status != 'Deleted'
),
extractions_additional AS (
    SELECT 
        id AS _id,
        sub_id,
        payment_plan_id AS _ppi,
        signed_date AS _sd,
        end_date AS _ed,
        row_number() OVER (PARTITION BY id ORDER BY sub_id ASC) AS first_record,
        lagInFrame(payment_plan_id) OVER (PARTITION BY id ORDER BY sub_id ASC) AS prev_payment_plan_id,
        lagInFrame(sub_id) OVER (PARTITION BY id ORDER BY sub_id ASC) AS prev_sub_id,
        dateDiff('day', ifnull(lagInFrame(end_date) OVER (PARTITION BY id ORDER BY sub_id ASC), signed_date)::date, signed_date::date) AS cnt_days
    FROM extraction
    WHERE subscription_type IN ('barter','main','trial')
),
transactions AS (
    SELECT
        total_amount,
        membership_fee,
        join_fee,
        id,
        updated_at,
        gate_id,
        employee_id,
        user_payment_plan_id,
        discount_amount,
        discount_code_id,
        discount_id,
        first_value(id) OVER (PARTITION BY user_payment_plan_id ORDER BY id) AS first_tr_id
    FROM ddxfitness_prod_v2.bi_completed_transactions
    WHERE provider_id IN (6, 17, 20, 21, 26, 27)
),
first_transactions AS (
    SELECT * FROM transactions
    WHERE id = first_tr_id
),
extraction_plus_transactions AS (
    SELECT
        e.discount_code_id,
        e.id,
        e.sub_id,
        e.subscription_type,
        e.payment_plan_id,
        e.signed_date,
        e.start_date,
        e.end_date,
        e.club_id,
        e.status,
        e.card_token_id,
        e.cancel_reason,
        tr.join_fee,
        tr.membership_fee,
        CASE
            WHEN e.sub_id < 1000000 THEN tr.total_amount + tr.membership_fee
            ELSE tr.total_amount
        END AS total_amount,
        e.discount_id,
        tr.id AS tr_id,
        tr.updated_at AS payment_date,
        tr.gate_id,
        tr.employee_id,
        tr.discount_amount,
        tr.discount_code_id AS discount_code_id_tr,
        tr.discount_id AS discount_id_tr
    FROM extraction AS e
    LEFT JOIN first_transactions AS tr ON tr.user_payment_plan_id = e.sub_id
),
inflow_info AS (
    SELECT
        first_id,
        is_new,
        is_inflow
    FROM ddxfitness_prod_v2.inflow_and_outflow_new
)
SELECT
    f.id AS user_id,
    f.sub_id,
    f.payment_plan_id,
    pp1.name AS subscription_name,
    f.subscription_type,
    multiIf(ea.first_record = 1, 'НЧК',
            subscription_type = 'sub', '-', 
            ea.cnt_days <= 365, 'БЧК < 1 года',
            ea.cnt_days > 365, 'БЧК > 1 года', '-') AS is_new,
    multiIf(ea.first_record = 1, 0, subscription_type = 'sub', 0, ea.cnt_days <= 0, 1, 0) AS is_resubbed,
    ii.is_new AS is_new_per_inflow,
    ii.is_inflow,
    ea.prev_sub_id,
    ea.prev_payment_plan_id,
    pp2.name AS prev_payment_plan_name,
    ur.phone,
    ur.email,
    multiIf(ur.birthday IS NULL, NULL,
            CAST(ur.birthday, 'UInt16') >= 39969,
            CAST(CAST(ur.birthday, 'UInt16') - 65536, 'date32'),
            ur.birthday) AS birthday,
    ur.sex,
    ur.sport_experience AS fitness_experience,
    f.signed_date,
    f.start_date,
    f.payment_date,
    f.end_date,
    c.name AS club_name,
    f.discount_code_id,
    dc1.code,
    ds1.name AS discount_name,
    f.card_token_id,
    f.status,
    f.join_fee,
    f.membership_fee,
    f.total_amount,
    f.cancel_reason,
    f.gate_id,
    f.employee_id,
    ur2.name AS employee_name,
    ur2.last_name,
    tg.name AS gate_name,
    f.discount_amount,
    multiIf(ds1.name LIKE '%Getblogger 04.2024%', 'Getblogger 04.2024',
            ds1.name LIKE '%Getblogger 05.2024%', 'Getblogger 05.2024',
            ds1.name LIKE '%Getblogger 06.2024%', 'Getblogger 06.2024',
            ds1.name LIKE '%Getblogger 07.2024%', 'Getblogger 07.2024',
            ds1.name LIKE '%Getblogger 08.2024%', 'Getblogger 08.2024',
            ds1.name LIKE '%Getblogger 09.2024%', 'Getblogger 09.2024',
            ds1.name LIKE '%Getblogger 10.2024%', 'Getblogger 10.2024',
            ds1.name LIKE '%Getblogger 11.2024%', 'Getblogger 11.2024',
            ds1.name LIKE '%Getblogger 12.2024%', 'Getblogger 12.2024',
            ds1.name LIKE '%GB именные 09.2024%', 'GB именные 09.2024',
            ds1.name LIKE '%Perfluence 06.2024%', 'Perfluence 06.2024',
            ds1.name LIKE '%Perfluence 10.2024%', 'Perfluence 10.2024',
            ds1.name LIKE '%Perfluence 11.2024%', 'Perfluence 11.2024',
            ds1.name LIKE '%Perfluence 12.2024%', 'Perfluence 12.2024',
            ds1.name LIKE '%PF именные 12.2024%', 'PF именные 12.2024',
            ds1.name LIKE '%MEGABD%', 'MEGABD 12.2024',
            ds1.name) AS discount_name_new,
    f.discount_code_id_tr,
    ds2.name AS discount_name_tr,
    f.discount_id_tr,
    dc2.code AS code_tr,
    ci.name AS city_name,
    now() + INTERVAL 3 HOUR AS updated_at
FROM extraction_plus_transactions AS f
LEFT JOIN extractions_additional ea ON f.sub_id = ea.sub_id
LEFT JOIN inflow_info ii ON f.sub_id = ii.first_id
LEFT JOIN ddxfitness_prod_v2.pg_users ur ON f.id = ur.id
LEFT JOIN ddxfitness_prod_v2.pg_employees em ON f.employee_id = em.id
LEFT JOIN ddxfitness_prod_v2.pg_users ur2 ON ur2.id = em.user_id
LEFT JOIN ddxfitness_prod_v2.pg_payment_plans pp1 ON pp1.id = f.payment_plan_id
LEFT JOIN ddxfitness_prod_v2.pg_payment_plans pp2 ON pp2.id = ea.prev_payment_plan_id
LEFT JOIN ddxfitness_prod_v2.pg_discounts ds1 ON ds1.id = f.discount_id
LEFT JOIN ddxfitness_prod_v2.pg_discounts ds2 ON ds2.id = f.discount_id_tr
LEFT JOIN ddxfitness_prod_v2.pg_discount_codes dc1 ON dc1.id = f.discount_code_id
LEFT JOIN ddxfitness_prod_v2.pg_discount_codes dc2 ON dc2.id = f.discount_code_id_tr
LEFT JOIN ddxfitness_prod_v2.pg_transaction_gates tg ON tg.id = f.gate_id
LEFT JOIN ddxfitness_prod_v2.pg_clubs c ON f.club_id = c.id
LEFT JOIN ddxfitness_prod_v2.pg_cities ci ON c.city_id = ci.id
WHERE 1 = 1;
