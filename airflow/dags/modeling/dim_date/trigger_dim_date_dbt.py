"""
Implement a DAG that triggers a dbt job to run the dim_date model.
"""

from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from datetime import datetime, timedelta

# Define the default arguments for the DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "trigger_dim_date_dbt",
    default_args=DEFAULT_ARGS,
    description="A DAG to trigger the dim_date dbt model",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["dbt"],
) as dag:

    # Define the start task
    start = EmptyOperator(task_id="start")

    # Define the dbt job task
    dbt_job = DbtCloudRunJobOperator(
        task_id="dbt_job",
        dbt_cloud_conn_id="dbt_cloud",
        job_id="523988",
    )

    # Define the end task
    end = EmptyOperator(task_id="end")

    # Define the task dependencies
    start >> dbt_job >> end
