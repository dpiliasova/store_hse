WITH confirmed_orders AS (
    SELECT
        customer_id,
        order_id,
        ordered_at,
        order_amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ordered_at, order_id
        ) AS order_number
    FROM {{ ref('int_orders_with_payments') }}
    WHERE is_confirmed AND NOT is_test_order
)

SELECT
    customer_id,
    MAX(order_id) FILTER (WHERE order_number = 1) AS first_confirmed_order_id,
    MAX(ordered_at) FILTER (WHERE order_number = 1) AS first_confirmed_at,
    COUNT(*) AS confirmed_orders_count,
    SUM(order_amount) AS confirmed_revenue
FROM confirmed_orders
GROUP BY customer_id
