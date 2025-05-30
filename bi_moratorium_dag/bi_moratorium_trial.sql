TRUNCATE TABLE ddxfitness_prod_v2.bi_moratorium_trial;

INSERT INTO ddxfitness_prod_v2.bi_moratorium_trial
WITH
    raw_statuses AS (
        SELECT
            id,
            club_id,
            pay_date,
            leadInFrame(pay_date) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS next_pay_date,
            updated_at,
            version,
            version_updated_at,
            status,
            lagInFrame(status) OVER (PARTITION BY id ORDER BY version_updated_at ASC, version ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS previous_status,
            cancel_reason
        FROM ddxfitness_prod_v2.user_payment_plans_history
        WHERE payment_plan_id IN (18, 20, 22, 92, 103, 118, 135, 162, 241, 242, 243)
          AND coalesce(cancel_reason, '') != 'ExcludeFromReport'
          AND updated_at >= '2023-12-01'
          AND coalesce(status, '') != 'Deleted'
          AND discount_id = 11538 
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
                coalesce(status, '') = 'Current' AND
                coalesce(previous_status, '') = 'Current' AND
                toDate(coalesce(pay_date, toDate('1970-01-01'))) != toDate(coalesce(next_pay_date, toDate('1970-01-01')))
            )
            OR (
                coalesce(status, '') = 'PaymentPending' AND
                coalesce(previous_status, '') IN ('Current', 'Freezed', '')
            )
            OR (
                coalesce(status, '') IN ('Current', 'Ended', 'Freezed', 'Refunded') AND
                coalesce(previous_status, '') = 'PaymentPending'
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
    )
SELECT
    id,
    club_id,
    cohort,
    cohort_date_time,
    cohort_end_date,
    cohort_close_status,
    end_type
FROM cohorts
WHERE cohort IS NOT NULL;
