# pylint: disable=unspecified-encoding

import os
import json
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# region Constants
PARENT_DIR = str(Path(__file__).parent)
SCHEMA_FILE_PATH = os.path.join(PARENT_DIR, "schema/chicago_taxi_trips.json")

GCS_BUCKET = "chicago_taxi_trips_test_2"

DESTINATION_PROJECT = os.environ["GCP_PROJECT"]
DESTINATION_DATASET = "chicago_taxi"
DESTINATION_MAIN_TABLE = "taxi_trips"
DESTINATION_STAGE_TABLE = f"{DESTINATION_MAIN_TABLE}_staging"

LOGICAL_DATE_YEAR = "{{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%Y')}}"
LOGICAL_DATE_MONTH = "{{macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%m')}}"
# endregion Constants

DEFAULT_ARGS = {
    "owner": "Data Engineering, Abdulrahman Bahaa",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2023, 1, 1),
    "gcp_conn_id": "google_cloud_default",
    "concurrency": 1,
    "max_active_runs": 1,
}

with DAG(
    "ingest_chicago_taxi_gcs_to_bq",
    default_args=DEFAULT_ARGS,
    description="A DAG to ingest chicago taxi data from GCS to BigQuery",
    schedule_interval="0 2 1 * *",  # Run the DAG at the first day of each month at 02:00 UTC
    tags=["chicago_taxi", "data_ingestion"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    ingest_to_bq_task = GCSToBigQueryOperator(
        task_id="ingest_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[f"{LOGICAL_DATE_YEAR}/{LOGICAL_DATE_MONTH}/*.json"],
        destination_project_dataset_table=f"{DESTINATION_PROJECT}.{DESTINATION_DATASET}.{DESTINATION_STAGE_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=json.load(open(SCHEMA_FILE_PATH))["schema"]["fields"],
    )

    merge_to_main_table_task = BigQueryInsertJobOperator(
        task_id="merge_to_main_table",
        configuration={
            "query": {
                "query": "{% include './sql/merge_to_main_table.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> ingest_to_bq_task >> merge_to_main_table_task >> end_task
