SELECT
    category_id,
    parent_category_id,
    category_name
FROM {{ source('shop', 'product_categories') }}
