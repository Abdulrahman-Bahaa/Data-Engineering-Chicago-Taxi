# pylint: disable=unspecified-encoding

import os
from pathlib import Path
import json

from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
from airflow.operators.empty import EmptyOperator

from airflow import DAG


PROJECT_ID = os.environ.get("GCP_PROJECT")
DATASET_ID = "chicago_taxi"
TABLE_ID = "taxi_trips"

PARENT_DIR = str(Path(__file__).parent)
SCHEMA_FILE_PATH = os.path.join(PARENT_DIR, "schema/chicago_taxi_trips.json")

DEFAULT_ARGS = {
    "owner": "Data Engineering, Abdulrahman Bahaa",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "catchup": False,
}

with DAG(
    dag_id="create_main_table",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
) as dag:

    start_task = EmptyOperator(task_id="start_task")

    create_main_chicago_taxi_table = BigQueryCreateEmptyTableOperator(
        task_id="create_main_chicago_taxi_table",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        table_resource=json.load(open(SCHEMA_FILE_PATH)),
    )

    end_task = EmptyOperator(task_id="end_task")

    start_task >> create_main_chicago_taxi_table >> end_task
