WITH item_totals AS (
    SELECT
        order_id,
        SUM(line_amount) AS items_amount
    FROM {{ ref('fct_order_items') }}
    GROUP BY order_id
)

SELECT
    o.order_id,
    o.order_amount,
    i.items_amount
FROM {{ ref('fct_orders') }} AS o
LEFT JOIN item_totals AS i
    ON o.order_id = i.order_id
WHERE i.order_id IS NULL
   OR o.order_amount IS DISTINCT FROM i.items_amount
