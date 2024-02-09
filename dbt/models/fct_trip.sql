{{ config(
    materialized='table',
    partition_by={
      "field": "trip_date",
      "data_type": "date",
      "granularity": "day"
    }
)}}

WITH fct_trip AS (
    SELECT
        unique_key,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        pickup_location,
        pickup_latitude,
        pickup_longitude,
        pickup_census_tract,
        pickup_community_area,
        pickup_location
        dropoff_location,
        dropoff_latitude,
        dropoff_longitude,
        dropoff_census_tract,
        dropoff_community_area,
        dim_date.date AS trip_date,
        dim_date.day_name AS trip_day_name,
        dim_date.day_of_week AS day_of_week,
        dim_date.day_of_month AS day_of_month,
        dim_date.is_weekend AS is_weekend
    FROM 
        `chicago-illinois-taxi-test-2.chicago_taxi.taxi_trips` trips
    LEFT JOIN
        `chicago-illinois-taxi-test-2.chicago_taxi.dim_date` dim_date ON CAST(FORMAT_DATE('%Y%m%d', DATE(trips.trip_end_timestamp)) AS INT64) = dim_date.date_id
)
SELECT * FROM fct_trip