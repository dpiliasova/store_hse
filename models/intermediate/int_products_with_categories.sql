SELECT
    p.product_id,
    p.product_name,
    p.brand,
    p.category AS category_name,
    c.category_id,
    c.parent_category_id,
    parent.category_name AS parent_category_name,
    parent.parent_category_id AS grandparent_category_id,
    grandparent.category_name AS grandparent_category_name,
    COALESCE(grandparent.category_name, parent.category_name, c.category_name)
        AS root_category_name
FROM {{ ref('stg_shop__products') }} AS p
LEFT JOIN {{ ref('stg_shop__product_categories') }} AS c
    ON p.category = c.category_name
LEFT JOIN {{ ref('stg_shop__product_categories') }} AS parent
    ON c.parent_category_id = parent.category_id
LEFT JOIN {{ ref('stg_shop__product_categories') }} AS grandparent
    ON parent.parent_category_id = grandparent.category_id
