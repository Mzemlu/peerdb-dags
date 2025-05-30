INSERT INTO ddxfitness_prod_v2.bi_change_completed_status_transaction 
WITH cte AS (
    SELECT
        id,
        status,
        leadInFrame(status) OVER (
            PARTITION BY id 
            ORDER BY version_updated_at ASC, version ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ns,
        leadInFrame(version_updated_at) OVER (
            PARTITION BY id 
            ORDER BY version_updated_at ASC, version ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ns_version_updated_at
    FROM ddxfitness_prod_v2.transactions_history
    WHERE id > COALESCE(
        (SELECT max(id) FROM ddxfitness_prod_v2.bi_change_completed_status_transaction),
        0
    )
)
SELECT
    id,
    status,
    ns,
    ns_version_updated_at
FROM cte
WHERE 
    status = 'completed'
    AND ns NOT IN ('completed', '');
