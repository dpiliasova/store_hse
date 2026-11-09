{{ config(distributed_replicated=true) }}

SELECT
    date_value::date AS date_day,
    EXTRACT(YEAR FROM date_value)::integer AS date_year,
    EXTRACT(QUARTER FROM date_value)::integer AS date_quarter,
    EXTRACT(MONTH FROM date_value)::integer AS date_month,
    EXTRACT(DAY FROM date_value)::integer AS day_of_month
FROM generate_series(
    timestamp '2026-01-01 00:00:00',
    timestamp '2026-12-31 00:00:00',
    interval '1 day'
) AS calendar(date_value)
