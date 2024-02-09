from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from airflow import DAG

from dags.modeling.config.dbt_job_config import DBT_JOB_ID

DEFAULT_ARGS = {
    "owner": "Data Management, Abdulrahman Bahaa",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


for dbt_job in DBT_JOB_ID.keys():
    with DAG(
        f"trigger_{dbt_job}",
        default_args=DEFAULT_ARGS,
        description=f"A DAG to trigger the {dbt_job} dbt model",
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        tags=["dbt"],
    ) as dag:

        start = EmptyOperator(task_id="start")

        dbt_job = DbtCloudRunJobOperator(
            task_id="dbt_job",
            dbt_cloud_conn_id="dbt_cloud",
            job_id=DBT_JOB_ID[dbt_job],
        )

        end = EmptyOperator(task_id="end")

        start >> dbt_job >> end
