TRUNCATE TABLE ddxfitness_prod_v2.bi_mobile_users;

INSERT INTO ddxfitness_prod_v2.bi_mobile_users
SELECT
    device_id,
    user_id,
    min(event_date) AS min_event_date,
    max(event_date) AS max_event_date 
FROM ddxfitness_prod_v2.bi_all_mobile_events bame 
WHERE user_id <> 0
GROUP BY device_id, user_id;
