DELETE FROM ddxfitness_prod_v2.bi_all_mobile_events 
WHERE receive_date >= '{date_from}';

INSERT INTO ddxfitness_prod_v2.bi_all_mobile_events
WITH main_plans AS (
    SELECT
        user_id,
        start_date,
        end_date,
        status,
        user_payment_plan_id,
        payment_plan,
        t2.name AS payment_plan_name
    FROM (
        SELECT
            user_id,
            signed_date::date AS start_date,
            coalesce(end_date, '2049-01-01')::date AS end_date,
            status,
            id AS user_payment_plan_id,
            payment_plan_id AS payment_plan
        FROM ddxfitness_prod_v2.pg_user_payment_plans
        WHERE payment_plan_id IN (SELECT id FROM ddxfitness_prod_v2.d_inflow_payment_plans)
          AND status NOT IN ('Created', 'Deleted')
          AND coalesce(cancel_reason, '') != 'ExcludeFromReport'
    ) t1
    INNER JOIN ddxfitness_prod_v2.pg_payment_plans t2 ON t1.payment_plan = t2.id
),
users_list AS (
    SELECT
        DeviceID,
        EventDate,
        EventDateTime,
        ReceiveDate,
        OSName,
        EventName,
        EventParameters,
        JSONExtractString(EventParameters, 'user_id') AS df,
        JSONExtractString(JSONExtractString(EventParameters, 'user'), 'id') AS nested_id,
        toInt64(multiIf(nested_id != '', nested_id, df != '', df, '0')) AS user_id_prepared,
        max(user_id_prepared) OVER (PARTITION BY DeviceID) AS user_id,
        row_number() OVER (PARTITION BY DeviceID) AS entry_num
    FROM mobile_db.events_all
    WHERE ReceiveDate >= '{date_from}' AND ReceiveDate < '{date_to}'
),
mobile_users AS (
    SELECT
        ul.DeviceID,
        ul.user_id,
        ul.EventDate,
        ul.EventDateTime,
        ul.OSName,
        ul.EventName,
        ul.EventParameters,
        ul.entry_num,
        u.sex,
        u.name,
        u.last_name,
        u.phone,
        u.email,
        u.birthday,
        u.sport_experience,
        u.home_club_id,
        c.name AS club_name,
        ul.ReceiveDate,
        mp.user_payment_plan_id,
        mp.start_date,
        mp.end_date
    FROM users_list ul
    LEFT JOIN ddxfitness_prod_v2.pg_users u ON ul.user_id = u.id
    LEFT JOIN ddxfitness_prod_v2.pg_clubs c ON u.home_club_id = c.id
    LEFT JOIN main_plans mp ON ul.user_id = mp.user_id
),
mobile_users_grouped AS (
    SELECT
        DeviceID,
        user_id,
        EventDate,
        EventDateTime,
        ReceiveDate,
        OSName,
        EventName,
        EventParameters,
        entry_num,
        sex,
        name,
        last_name,
        phone,
        email,
        birthday,
        sport_experience,
        home_club_id,
        club_name,
        max(user_payment_plan_id) AS user_payment_plan_id,
        min(start_date) AS start_date,
        max(end_date) AS end_date
    FROM mobile_users
    GROUP BY DeviceID, user_id, EventDate, EventDateTime, ReceiveDate,
             OSName, EventName, EventParameters, entry_num,
             sex, name, last_name, phone, email, birthday,
             sport_experience, home_club_id, club_name
)
SELECT
    mug.DeviceID,
    mug.user_id,
    mug.EventDate,
    mug.EventDateTime,
    mug.ReceiveDate,
    mug.OSName,
    mug.EventName,
    mug.EventParameters,
    mug.sex,
    mug.name,
    mug.last_name,
    mug.phone,
    mug.email,
    mug.birthday,
    mug.sport_experience,
    mug.home_club_id,
    mug.club_name,
    pp.name AS payment_plan_name,
    upp.status,
    mug.start_date,
    mug.end_date,
    mug.user_payment_plan_id,
    now() + INTERVAL 3 HOUR AS updated_at
FROM mobile_users_grouped mug
LEFT JOIN ddxfitness_prod_v2.pg_user_payment_plans upp ON mug.user_payment_plan_id = upp.id
LEFT JOIN ddxfitness_prod_v2.pg_payment_plans pp ON upp.payment_plan_id = pp.id;

OPTIMIZE TABLE ddxfitness_prod_v2.bi_all_mobile_events PARTITION tuple(toYYYYMM(toDate('{date_from}'))) FINAL;
