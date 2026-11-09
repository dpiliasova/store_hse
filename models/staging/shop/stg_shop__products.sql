SELECT
    product_id,
    product_name,
    brand,
    category
FROM {{ source('shop', 'products') }}
