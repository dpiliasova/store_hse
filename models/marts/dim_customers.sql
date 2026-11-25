{{ config(distributed_replicated=true) }}

SELECT
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    m.first_confirmed_order_id,
    m.first_confirmed_at,
    COALESCE(m.confirmed_orders_count, 0) AS confirmed_orders_count,
    COALESCE(m.confirmed_revenue, 0) AS confirmed_revenue,
    COALESCE(m.confirmed_orders_count, 0) > 0 AS is_buyer
FROM {{ ref('stg_shop__customers') }} AS c
LEFT JOIN {{ ref('int_customer_order_metrics') }} AS m
    ON c.customer_id = m.customer_id
