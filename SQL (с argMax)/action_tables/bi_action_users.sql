TRUNCATE TABLE ddxfitness_prod_v2.bi_action_users;

INSERT INTO ddxfitness_prod_v2.bi_action_users
WITH
    dedup_user_payment_plans AS (
        SELECT
            id,
            argMax(user_id, _peerdb_version) AS user_id,
            argMax(payment_plan_id, _peerdb_version) AS payment_plan_id,
            argMax(club_id, _peerdb_version) AS club_id,
            argMax(signed_date, _peerdb_version) AS signed_date,
            argMax(end_date, _peerdb_version) AS end_date,
            argMax(cancel_date, _peerdb_version) AS cancel_date,
            argMax(cancel_reason, _peerdb_version) AS cancel_reason,
            argMax(status, _peerdb_version) AS status,
            argMax(discount_id, _peerdb_version) AS discount_id
        FROM ddxfitness_prod_v2.pg_user_payment_plans
        GROUP BY id
    ),
    dedup_users AS (
        SELECT
            id,
            argMax(sex, _peerdb_version) AS sex,
            argMax(birthday, _peerdb_version) AS birthday
        FROM ddxfitness_prod_v2.pg_users
        GROUP BY id
    ),
    empl_list AS (
        SELECT user_id
        FROM (
            SELECT user_id
            FROM dedup_user_payment_plans AS upp
            WHERE payment_plan_id IN (1, 152, 208) AND status = 'Current'
            UNION DISTINCT
            SELECT user_id
            FROM ddxfitness_prod_v2.pg_employees AS e2
            WHERE is_active = 1
        )
    ),
    action_users AS (
        SELECT
            if_employee,
            transaction_date,
            min(transaction_date) OVER (PARTITION BY user_id ORDER BY transaction_id) min_transaction_date,
            transaction_id,
            signed_date,
            end_date,
            cancel_date,
            cancel_reason,
            return_transaction,
            return_transaction_date,
            type,
            user_id,
            provider_id,
            payment_plan_id,
            payment_plan,
            user_payment_plan_id,
            club_name,
            total_amount,
            discount_id,
            multiIf(discount_name = '', '-', discount_name) AS discount_name,
            current_status,
            gate_name,
            sex,
            multiIf(birthday IS NULL, NULL, CAST(birthday, 'UInt16') >= 39969, CAST(CAST(birthday, 'UInt16') - 65536, 'date32'), birthday) AS birthday,
            payment_plan_type,
            multiIf(payment_rank = 1, 'new', 'repeat') AS payment_type
        FROM (
            SELECT
                t.updated_at AS transaction_date,
                toTimezone(upp.signed_date, 'Europe/Moscow') + toIntervalHour(3) AS signed_date,
                toTimezone(upp.end_date, 'Europe/Moscow') + toIntervalHour(3) AS end_date,
                toTimezone(upp.cancel_date, 'Europe/Moscow') + toIntervalHour(3) AS cancel_date,
                upp.cancel_reason,
                upp.status AS current_status,
                t.id AS transaction_id,
                r.id AS return_transaction,
                r.return_transaction_date,
                t.user_id AS user_id,
                multiIf(e.user_id IS NULL, 'Клиент', 'Сотрудник') AS if_employee,
                provider_id,
                type,
                upp.payment_plan_id,
                pp.name AS payment_plan,
                t.user_payment_plan_id,
                c.name AS club_name,
                total_amount,
                t.discount_id AS discount_id,
                d.name AS discount_name,
                tg.name AS gate_name,
                us.sex AS sex,
                us.birthday AS birthday,
                pp2.payment_plan_type AS payment_plan_type,
                row_number() OVER (PARTITION BY t.user_payment_plan_id ORDER BY t.created_at ASC) AS payment_rank
            FROM ddxfitness_prod_v2.bi_completed_transactions AS t
            INNER JOIN dedup_user_payment_plans AS upp ON upp.id = t.user_payment_plan_id
            INNER JOIN ddxfitness_prod_v2.pg_clubs AS c ON c.id = upp.club_id
            INNER JOIN ddxfitness_prod_v2.pg_payment_plans AS pp ON upp.payment_plan_id = pp.id
            INNER JOIN ddxfitness_prod_v2.pg_transaction_gates AS tg ON t.gate_id = tg.id
            LEFT JOIN dedup_users AS us ON t.user_id = us.id
            LEFT JOIN ddxfitness_prod_v2.pg_payment_plans AS pp2 ON upp.payment_plan_id = pp2.id
            LEFT JOIN empl_list AS e ON upp.user_id = e.user_id
            LEFT JOIN ddxfitness_prod_v2.pg_discounts AS d ON t.discount_id = d.id
            LEFT JOIN (
                SELECT
                    id,
                    payment_transaction_id,
                    toTimezone(updated_at, 'Europe/Moscow') + toIntervalHour(3) AS return_transaction_date
                FROM ddxfitness_prod_v2.bi_completed_transactions AS t2
                WHERE type = 'refund'
            ) AS r ON t.id = r.payment_transaction_id
            WHERE upp.payment_plan_id IN (205, 206, 208) AND provider_id IN (1, 2, 6, 9, 10, 17, 20)
        ) AS ranked_transactions
        WHERE toDate(transaction_date) >= '2024-05-27'
    ),
    main_plans AS (
        SELECT
            user_id,
            CAST(main_pp_date, 'date') AS main_pp_date,
            user_payment_plan_id_main,
            user_payment_plan_main,
            discount_id_main,
            pp.name AS user_payment_plan_name_main,
            multiIf(d.name IN ('СТУДЕНЧЕСКИЙ 1000р', 'ТрейдИн 1000р', 'Трейд-ин 0 руб', 'День рождения'), d.name, '-') AS discount_name_main
        FROM (
            SELECT
                upp.user_id AS user_id,
                CAST(bct.updated_at, 'date') AS transaction_date,
                max(bct.updated_at) OVER (PARTITION BY upp.user_id, transaction_date) AS main_pp_date,
                max(upp.id) OVER (PARTITION BY upp.user_id, transaction_date) AS user_payment_plan_id_main,
                max(upp.payment_plan_id) OVER (PARTITION BY upp.user_id, transaction_date) AS user_payment_plan_main,
                max(upp.discount_id) OVER (PARTITION BY upp.user_id, transaction_date) AS discount_id_main
            FROM dedup_user_payment_plans AS upp
            LEFT JOIN (
                SELECT user_payment_plan_id, user_id, updated_at
                FROM ddxfitness_prod_v2.bi_completed_transactions AS bct
                WHERE provider_id IN (6, 17, 20, 21, 26, 27)
            ) AS bct ON upp.id = bct.user_payment_plan_id
            WHERE transaction_date != '1970-01-01'
              AND upp.payment_plan_id IN (
                  SELECT id FROM ddxfitness_prod_v2.d_inflow_payment_plans
              )
              AND status NOT IN ('Created', 'Deleted')
              AND coalesce(cancel_reason, '') != 'ExcludeFromReport'
        ) AS main
        LEFT JOIN ddxfitness_prod_v2.pg_payment_plans AS pp ON main.user_payment_plan_main = pp.id
        LEFT JOIN ddxfitness_prod_v2.pg_discounts AS d ON main.discount_id_main = d.id
        GROUP BY 1,2,3,4,5,6,7
    ),
    banner_users AS (
        SELECT
            max(EventDate) AS last_banner_date,
            JSONExtractRaw(EventParameters, 'user_id') AS user_id
        FROM ddxfitness_prod_v2.events_all AS ea
        WHERE EventName LIKE '%banner%' AND EventParameters LIKE '%\"banner_id\":78%'
        GROUP BY 2
    ),
    videos AS (
        SELECT
            vp.id AS video_progress_id,
            vp.video_id,
            vp.user_id,
            vp.duration,
            vp.stars,
            vp.difficulty,
            vp.created_at AS created_at,
            v.title AS video_name,
            v.type AS type_video,
            v.short_description,
            v.duration AS video_duration,
            v.is_deleted,
            v.is_free,
            row_number() OVER (PARTITION BY user_id ORDER BY created_at ASC) AS rn
        FROM ddxfitness_prod_v2.pg_video_progress AS vp
        LEFT JOIN ddxfitness_prod_v2.pg_videos AS v ON vp.video_id = v.id
        WHERE is_free = 1 AND is_deleted = 0 AND duration > 60
    ),
    trial_users AS (
        SELECT user_id
        FROM dedup_user_payment_plans
        WHERE payment_plan_id = 207
        GROUP BY user_id
    ),
    action_discounts AS (
        SELECT
            user_payment_plan_id,
            discount_name
        FROM action_users AS au
        WHERE payment_type = 'new'
        GROUP BY 1,2
    ),
    action_adv AS (
        SELECT user_id, max(click_date) click_date
        FROM ddxfitness_prod_v2.bi_adv_users
        GROUP BY 1
    ),
    main_plans_upd AS (
        SELECT
            user_payment_plan_id,
            mp.user_payment_plan_id_main,
            mp.user_payment_plan_name_main,
            mp.discount_name_main,
            mp.main_pp_date
        FROM action_users AS au
        LEFT JOIN main_plans AS mp ON au.user_id = mp.user_id AND CAST(au.transaction_date, 'date') = mp.main_pp_date
        WHERE payment_type = 'new'
        GROUP BY 1,2,3,4,5
    ),
    final_table AS (
        SELECT
            if_employee,
            transaction_date,
            transaction_id,
            signed_date,
            end_date,
            cancel_date,
            cancel_reason,
            return_transaction,
            return_transaction_date,
            type,
            t1.user_id AS user_id,
            provider_id,
            payment_plan_id,
            payment_plan,
            t1.user_payment_plan_id AS user_payment_plan_id,
            club_name,
            total_amount,
            discount_id,
            t6.discount_name AS discount_name,
            current_status,
            gate_name,
            sex,
            birthday,
            payment_plan_type,
            payment_type,
            t2.video_progress_id,
            t2.created_at,
            multiIf(t1.transaction_date > t2.created_at, 1, 0) AS buy_flag,
            dateDiff('day', t2.created_at, t1.transaction_date) AS diff_days,
            t3.user_payment_plan_id_main,
            t3.user_payment_plan_name_main,
            t3.discount_name_main,
            multiIf(t4.user_id = '', 'Не переходил', 'Перешел по баннеру') AS is_banner_user,
            t4.last_banner_date,
            multiIf(t5.user_id = 0, 'Не было триала', 'Был триал') AS is_trial_user,
            multiIf(t7.user_id <> 0 AND t7.click_date <= (t1.min_transaction_date)::date + INTERVAL 30 DAY, 'Пришел по трекеру', '-') AS is_tracker_user
        FROM action_users AS t1
        LEFT JOIN videos AS t2 ON (t1.user_id = t2.user_id) AND (rn = 1)
        LEFT JOIN main_plans_upd AS t3 ON t1.user_payment_plan_id = t3.user_payment_plan_id
        LEFT JOIN banner_users AS t4 ON toString(t1.user_id) = t4.user_id
        LEFT JOIN trial_users AS t5 ON t1.user_id = t5.user_id
        LEFT JOIN action_discounts AS t6 ON t1.user_payment_plan_id = t6.user_payment_plan_id
        LEFT JOIN action_adv AS t7 ON t1.user_id = t7.user_id
    )
SELECT *
FROM final_table
WHERE 1=1;
