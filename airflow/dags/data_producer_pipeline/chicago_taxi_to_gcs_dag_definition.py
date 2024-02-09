from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DEFAULT_ARGS = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2023, 1, 1),
    "gcp_conn_id": "google_cloud_default",
}

with DAG(
    "chicago_taxi_to_gcs",
    default_args=DEFAULT_ARGS,
    description="A DAG to load chicago taxi data into GCS",
    schedule_interval="@monthly",
    tags=["chicago_taxi", "data_producer"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    create_view_task = BigQueryInsertJobOperator(
        task_id="export_to_gcs",
        configuration={
            "query": {
                "query": "{% include './sql/export_filtered_data.sql' %}",
                "useLegacySql": False,
            },
        },
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> create_view_task >> end_task
