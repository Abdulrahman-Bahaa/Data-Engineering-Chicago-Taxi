WITH shift_duration AS (
    SELECT
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        LAG(trip_end_timestamp) OVER (PARTITION BY taxi_id ORDER BY trip_start_timestamp) AS previous_shift_end
    FROM `chicago-illinois-taxi-test-2.chicago_taxi.fct_trip`
)
SELECT
    taxi_id,
    COUNT(*) AS number_of_eligible_shifts
FROM
    shift_duration
WHERE
    TIMESTAMP_DIFF(trip_start_timestamp, previous_shift_end, HOUR) >= 8
GROUP BY
    taxi_id
ORDER BY
    number_of_eligible_shifts DESC
LIMIT 100