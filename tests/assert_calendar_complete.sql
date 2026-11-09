SELECT
    COUNT(*) AS total_days,
    COUNT(DISTINCT date_day) AS unique_days,
    MIN(date_day) AS first_day,
    MAX(date_day) AS last_day
FROM {{ ref('dim_dates') }}
HAVING COUNT(*) <> 365
    OR COUNT(DISTINCT date_day) <> 365
    OR MIN(date_day) IS DISTINCT FROM DATE '2026-01-01'
    OR MAX(date_day) IS DISTINCT FROM DATE '2026-12-31'
