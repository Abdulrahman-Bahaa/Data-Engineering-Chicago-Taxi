WITH dim_date AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS INT64) AS date_id,
        DATE_ADD(DATE '2020-01-01', INTERVAL n DAY) AS date,
        EXTRACT(YEAR FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS year,
        EXTRACT(MONTH FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS month,
        EXTRACT(DAY FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS day_of_month,
        EXTRACT(QUARTER FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS quarter,
        EXTRACT(DAYOFWEEK FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS day_of_week,
        FORMAT_DATE('%A', DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS day_name,
        EXTRACT(WEEK FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS week_of_year,
        EXTRACT(DAYOFYEAR FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) AS day_of_year,
        CASE WHEN EXTRACT(DAYOFWEEK FROM DATE_ADD(DATE '2020-01-01', INTERVAL n DAY)) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend
    FROM 
        UNNEST(GENERATE_ARRAY(0, DATE_DIFF(DATE '2025-01-01', DATE '2020-01-01', DAY))) AS n
)
SELECT * FROM dim_date