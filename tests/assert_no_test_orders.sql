SELECT
    'fct_orders' AS model_name,
    order_id
FROM {{ ref('fct_orders') }}
WHERE order_status = 'test'

UNION ALL

SELECT
    'fct_order_items' AS model_name,
    order_id
FROM {{ ref('fct_order_items') }}
WHERE order_status = 'test'
