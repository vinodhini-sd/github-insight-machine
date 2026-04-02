"""
DAG 3: Historical Backfill
===========================
Loads a configurable range of historical GH Archive hours.
Triggered manually with a date range config.

Example trigger config:
{"start_date": "2026-03-31", "end_date": "2026-04-01"}
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

GHARCHIVE_RAW_DATASET = Dataset("snowflake://VINO_GITHUB_INSIGHTS.RAW.GITHUB_EVENTS")
SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="gharchive_backfill",
    schedule=None,  # manual trigger only
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={
        "owner": "vino",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["github-insights", "ingestion", "backfill"],
    params={
        "start_date": "2026-03-31",
        "end_date": "2026-03-31",
    },
    doc_md=__doc__,
)
def gharchive_backfill():
    @task()
    def generate_hour_keys(**context):
        """Generate all hour keys for the date range."""
        from datetime import date

        params = context["params"]
        start = datetime.strptime(params["start_date"], "%Y-%m-%d").date()
        end = datetime.strptime(params["end_date"], "%Y-%m-%d").date()

        hour_keys = []
        current = start
        while current <= end:
            for hour in range(24):
                hour_keys.append(f"{current.isoformat()}-{hour}")
            current += timedelta(days=1)

        print(f"Generated {len(hour_keys)} hour keys: {hour_keys[0]} to {hour_keys[-1]}")
        return hour_keys

    @task(max_active_tis_per_dag=4)
    def ingest_hour(hour_key: str):
        """Call the Snowflake SP to ingest one hour."""
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        result = hook.run(
            f"CALL VINO_GITHUB_INSIGHTS.RAW.SP_INGEST_GHARCHIVE_HOUR('{hour_key}')"
        )
        print(f"Ingested {hour_key}: {result}")
        return hour_key

    @task(outlets=[GHARCHIVE_RAW_DATASET])
    def mark_complete(results):
        """Emit dataset update to trigger downstream dbt."""
        print(f"Backfill complete. Ingested {len(results)} hours.")

    hour_keys = generate_hour_keys()
    results = ingest_hour.expand(hour_key=hour_keys)
    mark_complete(results)


gharchive_backfill()
