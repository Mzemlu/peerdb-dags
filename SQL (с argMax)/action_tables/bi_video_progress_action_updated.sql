-- bi_video_progress_action_updated.sql

TRUNCATE TABLE ddxfitness_prod_v2.bi_video_progress_action;

INSERT INTO ddxfitness_prod_v2.bi_video_progress_action
WITH
    clean_historic_table AS (
        SELECT
            id,
            video_id,
            user_id,
            duration,
            is_completed,
            stars,
            difficulty,
            comment,
            toTimezone(updated_at, 'Europe/Moscow') + toIntervalHour(3) AS updated_at,
            toTimezone(updated_at, 'Europe/Moscow') + toIntervalHour(3) AS updated_at_plus_3,
            min(version_updated_at) AS version_updated_at,
            max(version) AS version
        FROM ddxfitness_prod_v2.video_progress_history AS vph
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    ),
new_id_table AS (
    SELECT
        max(id) OVER (PARTITION BY user_id, video_id) AS id,
        video_id,
        user_id,
        duration,
        is_completed,
        stars,
        difficulty,
        comment,
        updated_at,
        updated_at_plus_3,
        version_updated_at,
        version
    FROM clean_historic_table
),
list_video_progress AS (
    SELECT
        id,
        video_id,
        user_id,
        duration,
        is_completed,
        stars,
        difficulty,
        comment,
        updated_at,
        updated_at_plus_3,
        leadInFrame(updated_at) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_updated,
        lagInFrame(updated_at) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC) AS prev_updated,
        dateDiff('second', updated_at, leadInFrame(updated_at) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS second_between_next_session,
        dateDiff('second', lagInFrame(updated_at) OVER (PARTITION BY user_id,id ORDER BY updated_at ASC), updated_at) AS second_between_prev_session,
        multiIf((dateDiff('second', lagInFrame(updated_at) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC), updated_at) >= 10800) 
            OR (dateDiff('second', lagInFrame(updated_at) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC), updated_at) IS NULL), 
            1, 0) AS is_new_progress,
        version
    FROM new_id_table AS vph
    WHERE 1 = 1
    ORDER BY updated_at ASC
),
number_of_session_by_user AS (
    SELECT
        sum(is_new_progress) OVER (PARTITION BY user_id, id ORDER BY updated_at ASC) AS session_number,
        sum(is_new_progress) OVER (PARTITION BY user_id, video_id ORDER BY updated_at ASC) AS session_number_by_video,
        id,
        video_id,
        user_id,
        duration,
        is_completed,
        stars,
        difficulty,
        comment,
        updated_at,
        updated_at_plus_3,
        next_updated,
        prev_updated,
        second_between_next_session,
        second_between_prev_session,
        is_new_progress,
        version
    FROM list_video_progress
),
qty_rows_of_session_by_user AS (
    SELECT
        count(id) OVER (PARTITION BY user_id, id, session_number) AS qty_rows,
        session_number,
        id,
        video_id,
        user_id,
        duration,
        is_completed,
        stars,
        difficulty,
        comment,
        updated_at,
        updated_at_plus_3,
        next_updated,
        prev_updated,
        second_between_next_session,
        second_between_prev_session,
        is_new_progress,
        version
    FROM number_of_session_by_user
),
filter_random_viewing AS (
    SELECT *
    FROM qty_rows_of_session_by_user
    WHERE qty_rows >= 3
),
new_session_number AS (
    SELECT
        dense_rank() OVER (PARTITION BY user_id, id ORDER BY session_number ASC) AS new_session_number,
        *
    FROM filter_random_viewing
),
agregated_info_by_user AS (
    SELECT
        frv.new_session_number AS session_number,
        frv.id,
        v.title,
        v.type,
        v.is_free,
        v.duration AS duration,
        frv.video_id,
        frv.user_id,
        frv.qty_rows,            
        max(multiIf(frv.new_session_number > 1, 1, 0)) AS if_rewatch,
        max(CAST(coalesce(frv.updated_at, CAST('2049-01-01', 'date')), 'date')) AS updated_at,
        (sum(multiIf(second_between_next_session = 0, 0, 1)) * 17) + 30 AS duration_by_qty_rows,
        maxIf(frv.duration, frv.is_new_progress != 1) AS max_duration,
        max(frv.is_completed) AS is_completed,
        max(multiIf(frv.stars = 0, NULL, stars)) AS stars,
        max(multiIf(frv.difficulty = 0, NULL, difficulty)) AS difficulty,
        max(frv.comment) AS comment
    FROM new_session_number AS frv
    LEFT JOIN ddxfitness_prod_v2.pg_videos AS v ON frv.video_id = v.id
    GROUP BY 1,2,3,4,5,6,7,8,9
),
qty_rewatch_video_by_user AS (
    SELECT
        user_id,
        video_id,
        max(id) AS id,
        sum(if_rewatch) AS qty_rewatch
    FROM agregated_info_by_user
    GROUP BY 1,2
),
qty_user_rewatch_more_1 AS (
    SELECT
        video_id,
        countDistinct(user_id) AS qty_rewatch_more_1
    FROM qty_rewatch_video_by_user
    WHERE qty_rewatch > 1
    GROUP BY 1
),
max_rewatch_video_by_user AS (
    SELECT
        user_id,
        video_id,
        qty_rewatch,
        max(qty_rewatch) OVER (PARTITION BY video_id) AS max_rewatch
    FROM qty_rewatch_video_by_user
    GROUP BY 1,2,3
),
final_table AS (
    SELECT
        aibu.*, 
        mrvbu.max_rewatch,
        qurm1.qty_rewatch_more_1,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.user_payment_plan_id, 0)) AS user_payment_plan_id,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.payment_plan, '-')) AS payment_plan,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.start_date, CAST('1970-01-01','date'))) AS start_date,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.end_date, CAST('1970-01-01','date'))) AS end_date,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.gate_name, '-')) AS gate_name,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.if_employee, '-')) AS if_employee,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.discount_name, '-')) AS discount_name,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.discount_name_main, '-')) AS discount_name_main,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.is_banner_user, '-')) AS is_banner_user,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.current_status, '-')) AS current_status,
        max(multiIf((aibu.updated_at >= dd2.start_date) AND (aibu.updated_at <= dd2.end_date), dd2.is_trial_user, '-')) AS is_trial_user,
        max(dd2.is_tracker_user) AS is_tracker_user
    FROM agregated_info_by_user AS aibu
    LEFT JOIN max_rewatch_video_by_user AS mrvbu ON (aibu.video_id = mrvbu.video_id) AND (aibu.user_id = mrvbu.user_id)
    LEFT JOIN qty_user_rewatch_more_1 AS qurm1 ON aibu.video_id = qurm1.video_id
    LEFT JOIN (
        SELECT
            user_id,
            user_payment_plan_id,
            payment_plan,
            CAST(signed_date, 'date') AS start_date,
            CAST(coalesce(end_date, CAST('2049-01-01', 'date')), 'date') AS end_date,
            gate_name,
            if_employee,
            discount_name,
            discount_name_main,
            is_banner_user,
            current_status,
            is_trial_user,
            is_tracker_user
        FROM ddxfitness_prod_v2.bi_action_users
        WHERE payment_type = 'new'
    ) AS dd2 ON aibu.user_id = dd2.user_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
SELECT *
FROM final_table;
