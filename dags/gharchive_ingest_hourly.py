"""
DAG 1: GH Archive Hourly Ingestion
===================================
Runs every hour, calls the Snowflake stored procedure to fetch
that hour's GH Archive data and load into RAW.GITHUB_EVENTS.

Produces an Airflow Dataset that triggers the dbt transform DAG.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Airflow Dataset: downstream DAGs trigger when this is updated
GHARCHIVE_RAW_DATASET = Dataset("snowflake://VINO_GITHUB_INSIGHTS.RAW.GITHUB_EVENTS")

SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="gharchive_ingest_hourly",
    schedule="@hourly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={
        "owner": "vino",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["github-insights", "ingestion", "raw"],
    doc_md=__doc__,
)
def gharchive_ingest_hourly():
    @task()
    def compute_hour_key(**context):
        """Compute the GH Archive hour key for the previous hour."""
        # data_interval_start is the beginning of the interval this run covers
        logical_date = context["data_interval_start"]
        return logical_date.strftime("%Y-%m-%d-%-H")

    @task()
    def log_hour_key(hour_key: str):
        """Log which hour we're ingesting."""
        print(f"Ingesting GH Archive hour: {hour_key}")
        return hour_key

    hour_key = compute_hour_key()
    logged = log_hour_key(hour_key)

    ingest = SnowflakeOperator(
        task_id="call_ingest_sp",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="CALL VINO_GITHUB_INSIGHTS.RAW.SP_INGEST_GHARCHIVE_HOUR('{{ ti.xcom_pull(task_ids='compute_hour_key') }}')",
        outlets=[GHARCHIVE_RAW_DATASET],
    )

    logged >> ingest


gharchive_ingest_hourly()
