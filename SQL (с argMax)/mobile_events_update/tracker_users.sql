TRUNCATE TABLE ddxfitness_prod_v2.bi_adv_users;

INSERT INTO ddxfitness_prod_v2.bi_adv_users
SELECT
    ClickDate,
    extract(PostbackUrlParameters, 'tracker_device_id=(.*?)&') AS device_id,
    user_id
FROM mobile_db.postbacks_all pa
LEFT JOIN (
    SELECT user_id, device_id
    FROM (
        SELECT
            last_value(user_id) OVER (PARTITION BY user_id ORDER BY min_event_date) AS user_id,
            device_id
        FROM ddxfitness_prod_v2.bi_mobile_users
    )
    GROUP BY user_id, device_id
) mu ON device_id = mu.device_id
WHERE TrackerName IN (
    'action_from_main_banner_aos',
    'action_from_action__aos',
    'action_new_from_action__aos',
    'action_new_from_main_banner_aos'
)
AND device_id <> ''
AND user_id <> 0;
