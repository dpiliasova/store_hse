{{ config(distributed_by='order_id', orientation='column') }}

SELECT
    i.order_item_id,
    i.order_id,
    o.customer_id,
    i.product_id,
    o.order_date,
    o.order_status,
    o.is_confirmed,
    i.quantity,
    i.unit_price,
    i.discount_amount,
    i.gross_amount,
    i.line_amount
FROM {{ ref('stg_shop__order_items') }} AS i
INNER JOIN {{ ref('fct_orders') }} AS o
    ON i.order_id = o.order_id
