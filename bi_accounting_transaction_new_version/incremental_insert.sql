INSERT INTO ddxfitness_prod_v2.bi_accounting_transaction_new_version
WITH
    max_id AS (
        SELECT max(transaction_id) AS max_id
        FROM ddxfitness_prod_v2.bi_accounting_transaction_new_version
    ),
    count_receipts AS (
        SELECT
            toInt64OrNull(splitByString('_', CAST(number, 'String'))[2]) AS id_transaction,
            fiscal_status,
            count(*) AS cnt_receips
        FROM ddxfitness_prod_v2.pg_reciepts
        WHERE fiscal_status IN ('Commit', 'Awaiting success transact', 'Waiting commit')
        GROUP BY id_transaction, fiscal_status
    )
SELECT
    actr.transaction_id,
    cl.name AS club_name,
    actr.created_at + INTERVAL 6 HOUR AS date,
    multiIf(
        actr.provider_id = 0, '0',
        actr.provider_id = 1, 'Рекуррентная оплата подписки',
        actr.provider_id = 2, 'Оплата подписки',
        actr.provider_id = 6, 'Регистрация подписки',
        actr.provider_id = 3, 'Пополнение баланса',
        actr.provider_id = 5, 'Покупка с баланса',
        actr.provider_id = 7, 'Покупка продукта',
        actr.provider_id = 8, 'Возврат подписки',
        actr.provider_id = 9, 'Смена подписки',
        actr.provider_id = 11, 'Возврат покупки',
        actr.provider_id = 12, 'Покупка заморозки с депозита',
        actr.provider_id = 13, 'Покупка заморозки с карты',
        actr.provider_id = 14, 'Возврат покупки заморозки с депозита',
        actr.provider_id = 15, 'Возврат покупки заморозки с карты',
        actr.provider_id = 16, 'Смена платежной карты',
        actr.provider_id = 17, 'Регистрация подписки с баланса',
        actr.provider_id = 18, 'Возврат покупки с депозита',
        actr.provider_id = 19, 'Возврат подписки с депозита',
        NULL
    ) AS operation_name,
    actr.type AS transaction_type,
    actr.user_id,
    concat(u.name, ' ', u.last_name) AS name,
    actr.payment_plan_name AS subscription_name,
    actr.join_fee,
    actr.join_fee_net,
    actr.membership_fee,
    actr.membership_fee_net,
    multiIf(
        actr.payment_plan_tax = 0, '0',
        actr.payment_plan_tax = 1, 'ставка НДС 20%',
        actr.payment_plan_tax = 6, 'НДС не облагается',
        NULL
    ) AS vat,
    actr.product_name,
    actr.product_quantity AS product_qty,
    actr.product_price,
    actr.product_total_amount AS total_amount,
    actr.product_total_amount_net AS total_net,
    multiIf(
        actr.product_tax = 0, '0',
        actr.product_tax = 1, 'ставка НДС 20%',
        actr.product_tax = 6, 'НДС не облагается',
        NULL
    ) AS vat_num,
    actr.deposit_amount AS balance,
    tr.receipt_id AS receipt,
    multiIf(ss.id_transaction > 0, 1, re.cnt_receips) AS cnt_receips,
    coalesce(nullIf(ss.fiscal_status, ''), nullIf(re.fiscal_status, '')) AS fiscal_status
FROM ddxfitness_prod_v2.pg_accounting_transactions AS actr
LEFT JOIN ddxfitness_prod_v2.pg_users AS u ON actr.user_id = u.id
LEFT JOIN (
    SELECT *
    FROM ddxfitness_prod_v2.pg_transactions
    WHERE id > ifNull(
        (SELECT max(transaction_id) FROM ddxfitness_prod_v2.bi_accounting_transaction_new_version),
        0
    )
) AS tr ON actr.transaction_id = tr.id
LEFT JOIN ddxfitness_prod_v2.pg_club_legal_infos AS cli ON tr.club_legal_info_id = cli.id
LEFT JOIN ddxfitness_prod_v2.pg_clubs AS cl ON cli.club_id = cl.id
LEFT JOIN count_receipts AS re ON actr.transaction_id = re.id_transaction
LEFT JOIN count_receipts AS ss ON tr.parent_id = ss.id_transaction
CROSS JOIN max_id AS m
WHERE actr.is_deleted = false
  AND actr.transaction_id > ifNull(m.max_id, 0);
