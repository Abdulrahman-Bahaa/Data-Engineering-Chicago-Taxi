EXPORT DATA OPTIONS(
  uri="gs://chicago_taxi_trips_test_2/{{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')}}/{{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%m')}}/*.json",
  format='JSON',
  overwrite=true
  ) AS
    SELECT * FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    WHERE EXTRACT(YEAR FROM trip_start_timestamp) = {{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')}}
    AND EXTRACT(MONTH FROM trip_start_timestamp) = {{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%m')}}