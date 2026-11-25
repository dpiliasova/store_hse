{{ config(distributed_replicated=true) }}

WITH calendar AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min(order_date)::timestamp from " ~ ref('stg_shop__orders') ~ ")",
        end_date="(select max(order_date)::timestamp + interval '1 day' from " ~ ref('stg_shop__orders') ~ ")"
    ) }}
)

SELECT
    date_day::date AS date_day,
    EXTRACT(YEAR FROM date_day)::integer AS date_year,
    EXTRACT(QUARTER FROM date_day)::integer AS date_quarter,
    EXTRACT(MONTH FROM date_day)::integer AS date_month,
    EXTRACT(DAY FROM date_day)::integer AS day_of_month,
    EXTRACT(ISODOW FROM date_day)::integer AS day_of_week,
    EXTRACT(ISODOW FROM date_day) IN (6, 7) AS is_weekend
FROM calendar
