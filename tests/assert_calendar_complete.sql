WITH order_bounds AS (
    SELECT
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ ref('stg_shop__orders') }}
),

calendar_bounds AS (
    SELECT
        COUNT(*) AS total_days,
        COUNT(DISTINCT date_day) AS unique_days,
        MIN(date_day) AS first_day,
        MAX(date_day) AS last_day
    FROM {{ ref('dim_dates') }}
)

SELECT c.*, o.first_order_date, o.last_order_date
FROM calendar_bounds AS c
CROSS JOIN order_bounds AS o
WHERE c.total_days <> COALESCE(o.last_order_date - o.first_order_date + 1, 0)
   OR c.unique_days <> c.total_days
   OR c.first_day IS DISTINCT FROM o.first_order_date
   OR c.last_day IS DISTINCT FROM o.last_order_date
