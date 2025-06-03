TRUNCATE TABLE ddxfitness_prod_v2.inflow_and_outflow_marketing;

INSERT INTO ddxfitness_prod_v2.inflow_and_outflow_marketing (
    user_id, name, last_name, phone, email, birthday, sex, first_id, last_id,
    sport_experience, first_payment_plan_name, last_payment_plan_name, discount_name,
    discount_name_new, code, inflow_club_name, outflow_club_name, status, cancel_reason,
    is_new, start_date, start_training_date, end_date, discount_amount, membership_fee,
    qty_plans, user_status, pp_attribute, gate_name, employee_name, last_employee_name,
    end_date_value
)
WITH
    main AS (
        SELECT
            iaon.user_id,
            iaon.name,
            u.last_name,
            iaon.phone,
            iaon.email,
            iaon.birthday,
            iaon.sex,
            iaon.sport_experience,
            iaon.first_id,
            upp.last_id AS last_id_value,
            multiIf(iaon.last_id = -1, upp.last_id, iaon.last_id) AS last_id,
            multiIf(upp.last_id = 0, upp2.last_id, last_id) AS new_last_id,
            multiIf(iaon.last_id = -1, upp.last_payment_plan_name, iaon.last_payment_plan_name) AS last_payment_plan_name,
            multiIf(upp.last_id = 0, upp2.last_payment_plan_name, iaon.last_payment_plan_name) AS new_last_payment_plan_name,
            multiIf(iaon.last_id = -1, upp.outflow_club_name, iaon.outflow_club_name) AS outflow_club_name,
            multiIf(upp.last_id = 0, upp2.outflow_club_name, outflow_club_name) AS new_outflow_club_name,
            iaon.first_payment_plan_name,
            iaon.discount_name,
            multiIf(
                iaon.discount_name LIKE '%Getblogger 04.2024%', 'Getblogger 04.2024',
                iaon.discount_name LIKE '%Getblogger 05.2024%', 'Getblogger 05.2024',
                iaon.discount_name LIKE '%Getblogger 06.2024%', 'Getblogger 06.2024',
                iaon.discount_name LIKE '%Getblogger 07.2024%', 'Getblogger 07.2024',
                iaon.discount_name LIKE '%Getblogger 08.2024%', 'Getblogger 08.2024',
                iaon.discount_name LIKE '%Getblogger 09.2024%', 'Getblogger 09.2024',
                iaon.discount_name LIKE '%Getblogger 10.2024%', 'Getblogger 10.2024',
                iaon.discount_name LIKE '%Getblogger 11.2024%', 'Getblogger 11.2024',
                iaon.discount_name LIKE '%Getblogger 12.2024%', 'Getblogger 12.2024',
                iaon.discount_name LIKE '%GB именные 09.2024%', 'GB именные 09.2024',
                iaon.discount_name LIKE '%Perfluence 06.2024%', 'Perfluence 06.2024',
                iaon.discount_name LIKE '%Perfluence 10.2024%', 'Perfluence 10.2024',
                iaon.discount_name LIKE '%Perfluence 11.2024%', 'Perfluence 11.2024',
                iaon.discount_name LIKE '%Perfluence 12.2024%', 'Perfluence 12.2024',
                iaon.discount_name LIKE '%PF именные 12.2024%', 'PF именные 12.2024',
                iaon.discount_name LIKE '%MEGABD%', 'MEGABD 12.2024',
                iaon.discount_name
            ) AS discount_name_new,
            iaon.inflow_club_name,
            multiIf((last_id_value = 0) AND (iaon.end_date IS NULL), upp2.last_status, iaon.status) AS status,
            multiIf(iaon.last_id = -1, '-', iaon.cancel_reason) AS cancel_reason,
            iaon.start_date,
            iaon.start_training_date,
            multiIf((last_id_value = 0) AND (iaon.end_date IS NULL), today(), iaon.end_date) AS end_date,
            iaon.is_new AS original_is_new,
            mkt.discount_amount,
            mkt.membership_fee,
            iaon.qty_plans,
            iaon.user_status,
            multiIf(
                iaon.start_date = max(iaon.start_date) OVER (PARTITION BY user_id ORDER BY iaon.start_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                1, 0
            ) AS pp_attribute,
            mkt.gate_name,
            mkt.employee_name,
            mkt.last_name AS last_employee_name,
            toStartOfMonth(iaon.end_date) AS end_date_value
        FROM ddxfitness_prod_v2.inflow_and_outflow_new AS iaon
        LEFT JOIN ddxfitness_prod_v2.pg_users AS u ON iaon.user_id = u.id
        LEFT JOIN (
            SELECT * FROM (
                SELECT *, row_number() OVER (PARTITION BY sub_id ORDER BY sub_id ASC) AS rn
                FROM ddxfitness_prod_v2.bi_marketing_all_users
            ) WHERE rn = 1
        ) AS mkt ON iaon.first_id = mkt.sub_id
        LEFT JOIN (
            SELECT
                user_id,
                anyLast(upp.id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_id,
                anyLast(upp.payment_plan_id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_payment_plan_id,
                anyLast(upp.club_id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_club_id,
                pp.name AS last_payment_plan_name,
                c.name AS outflow_club_name
            FROM ddxfitness_prod_v2.pg_user_payment_plans AS upp
            LEFT JOIN ddxfitness_prod_v2.pg_payment_plans AS pp ON upp.payment_plan_id = pp.id
            LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c ON upp.club_id = c.id
            WHERE upp.payment_plan_id IN (SELECT id FROM ddxfitness_prod_v2.d_inflow_payment_plans)
              AND upp.status NOT IN ('Ended', 'Created', 'Deleted', 'Refunded')
        ) AS upp ON iaon.user_id = upp.user_id
        LEFT JOIN (
            SELECT
                user_id,
                anyLast(upp.id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_id,
                anyLast(upp.payment_plan_id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_payment_plan_id,
                anyLast(upp.club_id) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_club_id,
                anyLast(upp.status) OVER (PARTITION BY user_id ORDER BY upp.id ASC) AS last_status,
                pp.name AS last_payment_plan_name,
                c.name AS outflow_club_name
            FROM ddxfitness_prod_v2.pg_user_payment_plans AS upp
            LEFT JOIN ddxfitness_prod_v2.pg_payment_plans AS pp ON upp.payment_plan_id = pp.id
            LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c ON upp.club_id = c.id
            WHERE upp.payment_plan_id IN (SELECT id FROM ddxfitness_prod_v2.d_inflow_payment_plans)
              AND upp.status IN ('Ended', 'Refunded')
        ) AS upp2 ON iaon.user_id = upp2.user_id
    ),
    final_table AS (
        SELECT
            user_id, name, last_name, phone, email, birthday, sex, first_id, last_id,
            sport_experience, first_payment_plan_name, last_payment_plan_name,
            discount_name, discount_name_new, code, inflow_club_name, outflow_club_name,
            status, cancel_reason, start_date, start_training_date, end_date,
            discount_amount, membership_fee, qty_plans, user_status, pp_attribute,
            gate_name, employee_name, last_employee_name, end_date_value,
            multiIf(
                min(original_is_new) = 1, 'НЧК',
                dateDiff('day', lagInFrame(coalesce(end_date, toDate('1970-01-01'))) OVER (PARTITION BY user_id ORDER BY start_date ASC), start_date) <= 365, 'БЧК < 1 года',
                'БЧК > 1 года'
            ) AS is_new
        FROM main AS m
        LEFT JOIN (
            SELECT id, discount_code_id FROM ddxfitness_prod_v2.pg_user_payment_plans
        ) AS upp ON m.first_id = upp.id
        LEFT JOIN (
            SELECT id, code FROM ddxfitness_prod_v2.pg_discount_codes
        ) AS dc ON dc.id = upp.discount_code_id
        GROUP BY
            user_id, name, last_name, phone, email, birthday, sex, first_id, last_id,
            sport_experience, first_payment_plan_name, last_payment_plan_name,
            discount_name, discount_name_new, code, inflow_club_name, outflow_club_name,
            status, cancel_reason, start_date, start_training_date, end_date,
            discount_amount, membership_fee, qty_plans, user_status, pp_attribute,
            gate_name, employee_name, last_employee_name, end_date_value
    )
SELECT
    user_id, name, last_name, phone, email, birthday, sex, first_id, last_id,
    sport_experience, first_payment_plan_name, last_payment_plan_name, discount_name,
    discount_name_new, code, inflow_club_name, outflow_club_name, status, cancel_reason,
    is_new, start_date, start_training_date, end_date, discount_amount, membership_fee,
    qty_plans, user_status, pp_attribute, gate_name, employee_name, last_employee_name,
    end_date_value
FROM final_table;
