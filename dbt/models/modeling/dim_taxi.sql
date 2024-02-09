WITH dim_taxi AS (
    SELECT DISTINCT
        taxi_id,
        company
    FROM `chicago-illinois-taxi-test-2.chicago_taxi.taxi_trips`
)
SELECT * FROM dim_taxi
