TRUNCATE TABLE ddxfitness_prod_v2.bi_trainer_salary;

INSERT INTO ddxfitness_prod_v2.bi_trainer_salary
WITH
    cities_list AS (
        SELECT id,
               IF(name IN ('Москва','Красногорск','Мытищи','Одинцово','Химки',
                           'Зеленоград','Домодедово','Люберцы','Балашиха'), 'Москва', name) AS name
        FROM ddxfitness_prod_v2.pg_cities
    ),
    region_trainer_list AS (
        SELECT id,
               multiIf(left(trimBoth(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), '')), 1) != '7',
                       concat('+7', trimBoth(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), ''))),
                       concat('+', trimBoth(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), '')))) AS phone,
               name,
               category,
               regexpExtract(replaceAll(club, 'Х', 'X'), '^DDX ([\p{L}-]+)|([\p{L}-]+),\s*[\p{L}]+$', 1) AS city,
               c.id AS city_id
        FROM ddxfitness_prod_v2.trainer_list_with_types_region AS r
        LEFT JOIN ddxfitness_prod_v2.pg_cities AS c ON regexpExtract(replaceAll(club, 'Х', 'X'), '^DDX ([\p{L}-]+)|([\p{L}-]+),\s*[\p{L}]+$', 1) = c.name
        WHERE name != '' AND phone != '' AND id IS NOT NULL
    ),
    moscow_trainer_list AS (
        SELECT id,
               multiIf(left(trimBoth(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), '')), 1) != '7',
                       concat('+7', trimBoth(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), ''))),
                       concat('+', replaceAll(replaceRegexpAll(phone, concat('^[', regexpQuoteMeta('+'), ']+|[', regexpQuoteMeta('+'), ']+$'), ''), ' ', ''))) AS phone,
               name,
               category,
               'Москва' AS city,
               1 AS city_id
        FROM ddxfitness_prod_v2.trainer_list_with_types_moscow AS r
        WHERE name != '' AND phone != '' AND id IS NOT NULL
    ),
    group_by_phone AS (
        SELECT multiIf(u.id = 0, coalesce(mtl.id, rtl.id), u.id) AS id,
               multiIf(mtl.phone = '', rtl.phone, mtl.phone) AS phone,
               multiIf(mtl.name = '', rtl.name, mtl.name) AS name,
               multiIf(mtl.category = '', rtl.category, rtl.category) AS type
        FROM ddxfitness_prod_v2.pg_users AS u
        FULL OUTER JOIN moscow_trainer_list AS mtl ON u.phone = mtl.phone
        FULL OUTER JOIN region_trainer_list AS rtl ON u.phone = rtl.phone
    ),
    full_trainer_list AS (
        SELECT multiIf(gp.id != 0, gp.id, coalesce(mtl.id, rtl.id)) AS id,
               multiIf(gp.phone != '' AND gp.phone != '+7', gp.phone, mtl.phone) AS phone,
               multiIf(gp.name != '', gp.name, mtl.name) AS name,
               multiIf(gp.type != '', gp.type, mtl.category) AS type
        FROM group_by_phone AS gp
        FULL OUTER JOIN moscow_trainer_list AS mtl ON gp.id = mtl.id
        FULL OUTER JOIN region_trainer_list AS rtl ON gp.id = rtl.id
        WHERE phone != '' AND name != '' AND type != ''
        GROUP BY id, phone, name, type
    ),
    prepared_data AS (
        SELECT gttt.id AS timetable_id,
               gt.name AS training_name,
               cl.name AS club_name,
               c.name AS city_name,
               gtc.name AS category_name,
               gtc.id AS category_id,
               ur.id AS trainer_id,
               ur.phone AS phone,
               ur.last_name AS trainer_last_name,
               ur.name AS trainer_name,
               gttt.start_time AS start_time,
               gttt.end_time AS end_time,
               dateDiff('minute', toDateTime(gttt.start_time), toDateTime(gttt.end_time)) AS duration
        FROM ddxfitness_prod_v2.pg_group_training_time_tables AS gttt
        INNER JOIN ddxfitness_prod_v2.pg_group_trainings AS gt ON gt.id = gttt.group_training_id
        INNER JOIN ddxfitness_prod_v2.pg_clubs AS cl ON cl.id = gttt.club_id
        INNER JOIN cities_list AS c ON cl.city_id = c.id
        INNER JOIN ddxfitness_prod_v2.pg_group_training_categories AS gtc ON gt.group_training_category_id = gtc.id
        INNER JOIN ddxfitness_prod_v2.pg_group_training_employees AS gte ON gte.group_training_time_table_id = gttt.id
        INNER JOIN ddxfitness_prod_v2.pg_employees AS emp ON emp.id = gte.employee_id
        INNER JOIN ddxfitness_prod_v2.pg_users AS ur ON ur.id = emp.user_id
        WHERE gttt.is_deleted = false AND gttt.start_time >= '2024-01-01'
    ),
    final_table AS (
        SELECT pd.timetable_id,
               pd.training_name,
               pd.club_name,
               pd.city_name,
               pd.category_name,
               tt.type AS trainer_type,
               pd.trainer_id,
               pd.trainer_last_name,
               pd.trainer_name,
               pd.start_time,
               pd.duration,
               multiIf((category_id = 40 AND (duration = 30 OR duration = 55) AND arrayExists(x -> training_name ILIKE x, ['%HIIT%', '%GRIT%', '%CYCLE SPRINT@'])), ts.mk_55,
                       (category_id = 40 AND duration = 30), ts.mk_30,
                       (category_id = 40 AND duration = 55), ts.mk_55,
                       (category_id = 40 AND duration = 90), ts.mk_90,
                       arrayExists(x -> training_name ILIKE x, ['%YOGA%', '%ЙОГА%', '%Йога%', '%Yoga%']), ts.yoga,
                       arrayExists(x -> training_name ILIKE x, ['%CYCLE SPRINT@']) AND trainer_type = 'Звездный тренер сайкл', ts.cycle,
                       arrayExists(x -> training_name ILIKE x, ['%HIIT%', '%GRIT%', '%CYCLE SPRINT@']), ts.grit,
                       arrayExists(x -> training_name ILIKE x, ['%Cycle%']) AND trainer_type = 'Звездный тренер сайкл', ts.cycle,
                       duration = 30, ts.`30`,
                       duration = 45, ts.`45`,
                       duration = 55, ts.`55`,
                       0) AS salary
        FROM prepared_data AS pd
        LEFT JOIN (SELECT DISTINCT id, name, type, phone FROM full_trainer_list) AS tt ON pd.trainer_id = tt.id
        LEFT JOIN ddxfitness_prod_v2.trainer_salary AS ts ON tt.type = ts.trainer_type AND pd.city_name = ts.city
        WHERE pd.category_name NOT IN ('DDX Games', 'Smart Start', 'Игра «Элиас»', 'Игра «Мафия»',
                                       'Семинар «Женское здоровье и фитнес»', 'Соревнования «Железные люди»', '30 min Fullbody')
        GROUP BY timetable_id, training_name, club_name, city_name, category_name,
                 trainer_type, trainer_id, trainer_last_name, trainer_name, start_time, duration, salary
    )
SELECT *
FROM final_table;
