INSERT INTO ddxfitness_prod_v2.bi_change_completed_status_transaction 
WITH
    dedup_transactions_history AS (
        SELECT
            id,
            argMax(status, _peerdb_version) AS status,
            argMax(version_updated_at, _peerdb_version) AS version_updated_at,
            argMax(version, _peerdb_version) AS version
        FROM ddxfitness_prod_v2.transactions_history
        GROUP BY id
    ),
    cte AS (
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
        FROM dedup_transactions_history
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
