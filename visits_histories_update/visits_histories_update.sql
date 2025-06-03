INSERT INTO ddxfitness_prod_v2.bi_visits_histories
SELECT
    vh.id AS id,
    vh.user_id AS user_id,
    u.name AS user_name,
    coalesce(u.last_name, '') AS last_name,
    u.email AS email,
    coalesce(u.sex, '') AS sex,
    multiIf(
        u.birthday IS NULL, NULL,
        CAST(u.birthday, 'UInt16') >= 39969, CAST(CAST(u.birthday, 'UInt16') - 65536, 'date32'),
        u.birthday
    ) AS birthday,
    vh.club_id AS club_id,
    c.name AS club_name,
    toTimezone(vh.club_event_date, 'Europe/Moscow') AS event_date,
    vh.event_type AS event_type
FROM ddxfitness_prod_v2.pg_card_swipe_histories AS vh
LEFT JOIN ddxfitness_prod_v2.pg_clubs AS c ON vh.club_id = c.id
LEFT JOIN ddxfitness_prod_v2.pg_users AS u ON vh.user_id = u.id
WHERE vh.id > {last_id};
