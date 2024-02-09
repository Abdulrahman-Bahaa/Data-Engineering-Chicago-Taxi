import logging
from datetime import datetime

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow import DAG

DEFAULT_ARGS = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "retries": 1,
    "start_date": datetime(2013, 1, 1),
}


def generate_monthly_dates(start_date_str: str, end_date_str: str):
    # Parse input strings into datetime objects
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    current_date = start_date.replace(
        day=1
    )  # Start from the first day of the start month

    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")

        # Move to the first day of the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)


def trigger_dags_interval(**context):
    interval_start = context["dag_run"].conf["interval_start"]
    interval_end = context["dag_run"].conf["interval_end"]

    for date in generate_monthly_dates(interval_start, interval_end):
        logging.info(
            "Triggering the DAG %s with logical date %s",
            context["dag_run"].conf["dag_id"],
            date,
        )

        trigger_dag_id = context["dag_run"].conf["dag_id"]
        trigger_dag_run = TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=trigger_dag_id,
            execution_date=date,
        )
        trigger_dag_run.execute(dict())


with DAG(
    "trigger_dags_interval",
    default_args=DEFAULT_ARGS,
    description="A DAG to trigger the ingestion DAG at the first day of each month in the given interval",
    schedule_interval=None,
    tags=["generic_dags"],
    params={"dag_id": "", "interval_start": "2023-01-01", "interval_end": "2023-12-01"},
) as dag:

    start_task = EmptyOperator(task_id="start")

    trigger_dags_task = PythonOperator(
        task_id="trigger_dags",
        python_callable=trigger_dags_interval,
        provide_context=True,
    )

    start_task >> trigger_dags_task
