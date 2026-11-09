SELECT
    o.order_id,
    o.order_status,
    o.is_confirmed
FROM {{ ref('fct_orders') }} AS o
WHERE o.is_confirmed IS DISTINCT FROM (
    o.order_status = 'paid'
    AND EXISTS (
        SELECT 1
        FROM {{ source('shop', 'payments') }} AS p
        WHERE p.order_id = o.order_id
          AND p.payment_status = 'success'
          AND p.amount >= 0
    )
)
