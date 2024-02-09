WITH trip_counts AS (
    SELECT
        trip_date,
        public_holiday_name,
        is_weekend,
        COUNT(unique_key) AS num_trips
    FROM
        `chicago-illinois-taxi-test-2.chicago_taxi.fct_trip`
    GROUP BY
        trip_date, public_holiday_name, is_weekend
)
SELECT
    CASE
        WHEN public_holiday_name IS NOT NULL THEN 'Holiday'
        WHEN is_weekend = 1 THEN 'Weekend'
        ELSE 'Normal Day'
    END AS day_type,
    CAST(AVG(num_trips) AS INT64) AS avg_trips
FROM
    trip_counts
GROUP BY
    day_type
ORDER BY
    day_type