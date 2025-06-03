DELETE FROM ddxfitness_prod_v2.agg_24h_clubs_sessions
WHERE check_time >= toDateTime('{date_from}', 'Europe/Moscow')
  AND check_time < toDateTime('{date_to}', 'Europe/Moscow');

INSERT INTO ddxfitness_prod_v2.agg_24h_clubs_sessions
WITH
    dedup_users AS (
        SELECT
            id,
            argMax(sex, _peerdb_version) AS sex
        FROM ddxfitness_prod_v2.pg_users
        GROUP BY id
    ),
    clubs_24h AS (
        SELECT
            id AS club_id,
            name AS club_name
        FROM ddxfitness_prod_v2.pg_clubs
        WHERE JSONExtractRaw(open_hours, 'is_open_all_day') = 'true'
    ),
    employees_list AS (
        SELECT user_id
        FROM ddxfitness_prod_v2.pg_employees
        WHERE is_deleted = 0 AND is_active = 1
    ),
    base_events AS (
        SELECT DISTINCT
            vh.user_id,
            vh.club_id,
            vh.club_name,
            COALESCE(u.sex, 'unknown') AS sex,
            vh.event_type,
            vh.event_date
        FROM ddxfitness_prod_v2.bi_visits_histories vh
        LEFT JOIN dedup_users u ON u.id = vh.user_id
        WHERE vh.event_type IN ('club_in', 'club_out')
          AND vh.event_date >= toDateTime('{date_from}', 'Europe/Moscow') - INTERVAL 2 HOUR
          AND vh.event_date < toDateTime('{date_to}', 'Europe/Moscow') + INTERVAL 2 HOUR
          AND vh.user_id NOT IN (SELECT user_id FROM employees_list)
          AND vh.club_id IN (SELECT club_id FROM clubs_24h)
    ),
    sorted_events AS (
        SELECT
            user_id,
            club_id,
            club_name,
            sex,
            event_type,
            event_date,
            ROW_NUMBER() OVER (PARTITION BY user_id, club_id ORDER BY event_date) AS rn
        FROM base_events
    ),
    sessions AS (
        SELECT
            se.user_id,
            se.club_id,
            se.club_name,
            se.sex,
            se.event_date AS start_time,
            COALESCE(
                CASE
                    WHEN nxt.event_type = 'club_out' THEN nxt.event_date
                    WHEN nxt.event_type = 'club_in' THEN nxt.event_date - INTERVAL 1 SECOND
                    ELSE NULL
                END,
                se.event_date + INTERVAL 2 HOUR
            ) AS end_time
        FROM sorted_events se
        LEFT JOIN sorted_events nxt
            ON nxt.user_id = se.user_id
            AND nxt.club_id = se.club_id
            AND nxt.rn = se.rn + 1
        WHERE se.event_type = 'club_in'
    ),
    sessions_with_hours AS (
        SELECT
            user_id,
            club_id,
            club_name,
            sex,
            start_time,
            end_time,
            arrayJoin(
                arrayMap(
                    x -> toStartOfHour(
                        toDateTime('{date_from}', 'Europe/Moscow') - INTERVAL 2 HOUR + x * 3600
                    ),
                    range(
                        toUInt32(
                            (toUnixTimestamp(toStartOfHour(start_time))
                             - toUnixTimestamp(toDateTime('{date_from}', 'Europe/Moscow') - INTERVAL 2 HOUR)
                            ) / 3600
                        ),
                        toUInt32(
                            (toUnixTimestamp(toStartOfHour(end_time))
                             - toUnixTimestamp(toDateTime('{date_from}', 'Europe/Moscow') - INTERVAL 2 HOUR)
                            ) / 3600 + 1
                        )
                    )
                )
            ) AS date_hr
        FROM sessions
        WHERE toStartOfHour(end_time) >= toDateTime('{date_from}', 'Europe/Moscow') - INTERVAL 2 HOUR
          AND toStartOfHour(start_time) < toDateTime('{date_to}', 'Europe/Moscow') + INTERVAL 2 HOUR
    ),
    hourly_qty AS (
        SELECT
            club_id,
            club_name,
            date_hr,
            sex,
            COUNT(DISTINCT user_id) AS people_in_club
        FROM sessions_with_hours
        GROUP BY club_id, club_name, date_hr, sex
    ),
    hourly_ext AS (
        SELECT
            club_id,
            club_name,
            date_hr,
            'total' AS sex,
            SUM(people_in_club) AS people_in_club
        FROM hourly_qty
        GROUP BY club_id, club_name, date_hr
        UNION ALL
        SELECT
            club_id,
            club_name,
            date_hr,
            sex,
            people_in_club
        FROM hourly_qty
    )
SELECT
    date_hr AS check_time,
    club_id,
    club_name,
    sex,
    people_in_club
FROM hourly_ext
WHERE date_hr >= toDateTime('{date_from}', 'Europe/Moscow')
  AND date_hr < toDateTime('{date_to}', 'Europe/Moscow')
ORDER BY club_name, date_hr, sex;

OPTIMIZE TABLE ddxfitness_prod_v2.agg_24h_clubs_sessions
PARTITION tuple(toYYYYMM(toDate('{date_from}'))) FINAL;
