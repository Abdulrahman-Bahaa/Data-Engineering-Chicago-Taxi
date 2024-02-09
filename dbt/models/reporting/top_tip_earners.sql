WITH top_tip_earners AS (
    SELECT
        taxi_id,
        ROUND(SUM(tips), 2) AS total_tips
    FROM 
        `chicago-illinois-taxi-test-2.chicago_taxi.fct_trip`
    WHERE
        trip_date BETWEEN CURRENT_DATE() - INTERVAL '3' MONTH AND CURRENT_DATE()
    GROUP BY
        taxi_id
)
SELECT
    taxi_id,
    total_tips
FROM
    top_tip_earners
ORDER BY
    total_tips DESC
LIMIT 100