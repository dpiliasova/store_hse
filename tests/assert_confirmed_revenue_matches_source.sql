WITH source_totals AS (
    SELECT
        COUNT(*) AS orders_count,
        COALESCE(SUM(o.total_amount), 0) AS revenue
    FROM {{ source('shop', 'orders') }} AS o
    WHERE o.status = 'paid'
      AND EXISTS (
          SELECT 1
          FROM {{ source('shop', 'payments') }} AS p
          WHERE p.order_id = o.order_id
            AND p.payment_status = 'success'
            AND p.amount >= 0
      )
),

mart_totals AS (
    SELECT
        COUNT(*) AS orders_count,
        COALESCE(SUM(order_amount), 0) AS revenue
    FROM {{ ref('fct_orders') }}
    WHERE is_confirmed
)

SELECT
    s.orders_count AS source_orders_count,
    m.orders_count AS mart_orders_count,
    s.revenue AS source_revenue,
    m.revenue AS mart_revenue
FROM source_totals AS s
CROSS JOIN mart_totals AS m
WHERE s.orders_count IS DISTINCT FROM m.orders_count
   OR s.revenue IS DISTINCT FROM m.revenue
