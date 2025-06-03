DELETE FROM ddxfitness_prod_v2.bi_mobile_stories 
WHERE receive_date >= '{date_from}';

INSERT INTO ddxfitness_prod_v2.bi_mobile_stories
SELECT
    device_id,
    event_date,
    event_datetime,
    receive_date,
    os_name,
    event_name,
    JSONExtractString(event_params, 'story_id') AS story_id, 
    JSONExtractString(event_params, 'slide_id') AS slide_id,
    t2.header,
    JSONExtractString(event_params, 'showtime') AS showtime,
    user_id,
    sex,
    name,
    last_name,
    phone,
    email,
    birthday,
    sport_experience,
    club_name,
    payment_plan_name,
    status,
    user_payment_plan_id,
    now() + INTERVAL 3 HOUR
FROM ddxfitness_prod_v2.bi_all_mobile_events t1 FINAL
LEFT JOIN ddxfitness_prod_v2.pg_story_slides t2
    ON JSONExtractString(t1.event_params, 'slide_id') = toString(t2.id)
WHERE event_name IN (
    'stories_get_error',
    'stories_views_error',
    'stories_preview_load_error',
    'stories_slide_background_load_error',
    'stories_answers_error',
    'stories_open_route_error',
    'stories_open',
    'stories_closed',
    'stories_hold'
)
AND t1.receive_date >= '{date_from}' 
AND t1.receive_date < '{date_to}';
