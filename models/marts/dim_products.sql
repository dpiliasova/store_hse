{{ config(distributed_replicated=true) }}

SELECT
    product_id,
    product_name,
    brand,
    category_id,
    category_name,
    parent_category_id,
    parent_category_name,
    grandparent_category_id,
    grandparent_category_name,
    root_category_name
FROM {{ ref('int_products_with_categories') }}
