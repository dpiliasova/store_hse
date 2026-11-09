{{ config(distributed_replicated=true) }}

SELECT
    customer_id,
    registered_at,
    registration_date,
    city,
    acquisition_channel
FROM {{ ref('stg_shop__customers') }}
