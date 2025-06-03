TRUNCATE TABLE ddxfitness_prod_v2.bi_moratorium;

INSERT INTO ddxfitness_prod_v2.bi_moratorium
WITH
    dedup_user_payment_plans_history AS (
        SELECT
            id,
            argMax(club_id, _peerdb_version) AS club_id,
            argMax(pay_date, _peerdb_version) AS pay_date,
            argMax(updated_at, _peerdb_version) AS updated_at,
            argMax(version, _peerdb_version) AS version,
            argMax(version_updated_at, _peerdb_version) AS version_updated_at,
            argMax(status, _peerdb_version) AS status,
            argMax(cancel_reason, _peerdb_version) AS cancel_reason
        FROM ddxfitness_prod_v2.user_payment_plans_history
        GROUP BY id
    ),
    raw_statuses AS (
        SELECT
            id,
            club_id,
            pay_date,
            leadInFrame(pay_date) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC) AS next_pay_date,
            updated_at,
            version,
            version_updated_at,
            status,
            lagInFrame(status) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC) AS previous_status,
            cancel_reason
        FROM dedup_user_payment_plans_history
        WHERE payment_plan_id IN (18, 20, 22, 92, 103, 118, 135, 162, 241, 242, 243)
          AND coalesce(cancel_reason, '') != 'ExcludeFromReport'
          AND updated_at >= '2023-12-01'
          AND coalesce(status, '') != 'Deleted'
    ),
    pre_moratorium AS (
        SELECT
            id,
            club_id,
            CAST(coalesce(pay_date, toDateTime('1970-01-01 00:00:00')) AS Date) AS cohort,
            coalesce(pay_date, toDateTime('1970-01-01 00:00:00')) AS cohort_date_time,
            leadInFrame(updated_at) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC) AS cohort_end_date_time,
            status,
            previous_status,
            pay_date,
            next_pay_date,
            leadInFrame(status) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC) AS next_status,
            leadInFrame(cancel_reason) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC) AS next_cancel_reason,
            multiIf(
                coalesce(status, '') = 'PaymentPending', 'open',
                (coalesce(status, '') = 'Current') AND (coalesce(previous_status, '') = 'Current'), 'continue',
                'close'
            ) AS cohort_status
        FROM raw_statuses
        WHERE (
            (
                coalesce(status, '') = 'Current'
                AND coalesce(previous_status, '') = 'Current'
                AND toDate(coalesce(pay_date, toDate('1970-01-01')))
                  != toDate(coalesce(next_pay_date, toDate('1970-01-01')))
            )
            OR (
                coalesce(status, '') = 'PaymentPending'
                AND coalesce(previous_status, '') IN ('Current', 'Freezed', '')
            )
            OR (
                coalesce(status, '') IN ('Current', 'Ended', 'Freezed', 'Refunded')
                AND coalesce(previous_status, '') = 'PaymentPending'
            )
        )
    ),
    cohorts AS (
        SELECT
            id,
            club_id,
            cohort,
            cohort_date_time,
            multiIf(cohort_status = 'continue', cohort_date_time, cohort_end_date_time) AS cohort_end_date,
            multiIf(
                cohort_status = 'continue', 'Continue',
                next_status = 'Refunded', 'Ended',
                next_status
            ) AS cohort_close_status,
            multiIf(
                next_status = 'Refunded', 'self',
                cohort_close_status = 'Ended',
                multiIf(coalesce(next_cancel_reason, '') = 'AutoCanceledByDebt', 'auto', 'self'),
                NULL
            ) AS end_type
        FROM pre_moratorium
        WHERE cohort_status != 'close'
          AND cohort > toDate('1970-01-01')
    ),
    cohorts_days AS (
        SELECT
            id,
            club_id,
            cohort,
            cohort_date_time,
            cohort_end_date,
            multiIf(
                cohort_date_time = cohort_end_date,
                0,
                floor(dateDiff('minute', cohort_date_time, cohort_end_date) / 1440) + 1
            ) AS cohort_period_days,
            cohort_date_time + toIntervalDay(1) AS inter1,
            cohort_date_time + toIntervalDay(2) AS inter2,
            cohort_date_time + toIntervalDay(3) AS inter3,
            cohort_date_time + toIntervalDay(4) AS inter4,
            cohort_date_time + toIntervalDay(5) AS inter5,
            cohort_date_time + toIntervalDay(6) AS inter6,
            cohort_date_time + toIntervalDay(7) AS inter7,
            cohort_date_time + toIntervalDay(8) AS inter8,
            cohort_date_time + toIntervalDay(9) AS inter9,
            cohort_date_time + toIntervalDay(10) AS inter10,
            cohort_date_time + toIntervalDay(11) AS inter11,
            multiIf(cohort_close_status = 'Continue', false, true) AS `1`,
            multiIf(cohort_close_status = 'Continue', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter1 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `1status`,
            multiIf(cohort_end_date IS NULL, true, inter1 < cohort_end_date, true, false) AS `2`,
            multiIf(`1status` IS NULL, NULL, `1status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter2 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `2status`,
            multiIf(cohort_end_date IS NULL, true, inter2 < cohort_end_date, true, false) AS `3`,
            multiIf(`2status` IS NULL, NULL, `2status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter3 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `3status`,
            multiIf(cohort_end_date IS NULL, true, inter3 < cohort_end_date, true, false) AS `4`,
            multiIf(`3status` IS NULL, NULL, `3status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter4 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `4status`,
            multiIf(cohort_end_date IS NULL, true, inter4 < cohort_end_date, true, false) AS `5`,
            multiIf(`4status` IS NULL, NULL, `4status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter5 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `5status`,
            multiIf(cohort_end_date IS NULL, true, inter5 < cohort_end_date, true, false) AS `6`,
            multiIf(`5status` IS NULL, NULL, `5status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter6 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `6status`,
            multiIf(cohort_end_date IS NULL, true, inter6 < cohort_end_date, true, false) AS `7`,
            multiIf(`6status` IS NULL, NULL, `6status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter7 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `7status`,
            multiIf(cohort_end_date IS NULL, true, inter7 < cohort_end_date, true, false) AS `8`,
            multiIf(`7status` IS NULL, NULL, `7status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter8 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `8status`,
            multiIf(cohort_end_date IS NULL, true, inter8 < cohort_end_date, true, false) AS `9`,
            multiIf(`8status` IS NULL, NULL, `8status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter9 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `9status`,
            multiIf(cohort_end_date IS NULL, true, inter9 < cohort_end_date, true, false) AS `10`,
            multiIf(`9status` IS NULL, NULL, `9status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter10 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `10status`,
            multiIf(cohort_end_date IS NULL, true, inter10 < cohort_end_date, true, false) AS `11`,
            multiIf(`10status` IS NULL, NULL, `10status` != 'Pending', NULL,
                    cohort_end_date IS NULL, 'Pending',
                    inter11 < cohort_end_date, 'Pending',
                    cohort_close_status) AS `11status`,
            multiIf(`11status` = 'Pending', 'Pending', cohort_close_status) AS cohort_final_status,
            end_type
        FROM cohorts
        WHERE cohort_end_date IS NULL
           OR cohort_date_time <= cohort_end_date
           OR toDate(coalesce_date_time) = toDate(cohort_end_date)
    ),
    cohorts_aggregates AS (
        SELECT
            cohort,
            club_id,
            count(*) AS all_subs_due,
            countIf(`1`) AS day1,
            countIf(`2`) AS day2,
            countIf(`3`) AS day3,
            countIf(`4`) AS day4,
            countIf(`5`) AS day5,
            countIf(`6`) AS day6,
            countIf(`7`) AS day7,
            countIf(`8`) AS day8,
            countIf(`9`) AS day9,
            countIf(`10`) AS day10,
            countIf(`11`) AS day11,
            countIf(`1status` = 'Current') AS current1,
            countIf(`1status` = 'Freezed') AS freezed1,
            countIf(`1status` = 'Ended') AS ended1,
            countIf(`2status` = 'Current') AS current2,
            countIf(`2status` = 'Freezed') AS freezed2,
            countIf(`2status` = 'Ended') AS ended2,
            countIf(`3status` = 'Current') AS current3,
            countIf(`3status` = 'Freezed') AS freezed3,
            countIf(`3status` = 'Ended') AS ended3,
            countIf(`4status` = 'Current') AS current4,
            countIf(`4status` = 'Freezed') AS freezed4,
            countIf(`4status` = 'Ended') AS ended4,
            countIf(`5status` = 'Current') AS current5,
            countIf(`5status` = 'Freezed') AS freezed5,
            countIf(`5status` = 'Ended') AS ended5,
            countIf(`6status` = 'Current') AS current6,
            countIf(`6status` = 'Freezed') AS freezed6,
            countIf(`6status` = 'Ended') AS ended6,
            countIf(`7status` = 'Current') AS current7,
            countIf(`7status` = 'Freezed') AS freezed7,
            countIf(`7status` = 'Ended') AS ended7,
            countIf(`8status` = 'Current') AS current8,
            countIf(`8status` = 'Freezed') AS freezed8,
            countIf(`8status` = 'Ended') AS ended8,
            countIf(`9status` = 'Current') AS current9,
            countIf(`9status` = 'Freezed') AS freezed9,
            countIf(`9status` = 'Ended') AS ended9,
            countIf(`10status` = 'Current') AS current10,
            countIf(`10status` = 'Freezed') AS freezed10,
            countIf(`10status` = 'Ended') AS ended10,
            countIf(`11status` = 'Current') AS current11,
            countIf(`11status` = 'Freezed') AS freezed11,
            countIf(`11status` = 'Ended') AS ended11,
            countIf(`11status` = 'Pending') AS pending11,
            countIf(end_type = 'auto') AS auto,
            countIf(end_type = 'self') AS self
        FROM cohorts_days
        GROUP BY cohort, club_id
    )
SELECT
    fq.*,
    cl.name AS club_name
FROM cohorts_aggregates AS fq
INNER JOIN ddxfitness_prod_v2.pg_clubs AS cl
    ON fq.club_id = cl.id
WHERE 1 = 1;
