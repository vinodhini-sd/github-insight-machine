"""
DAG 4: Repo Metadata Enrichment
================================
Triggered by the same Airflow Dataset as the dbt DAG (new raw data).
Calls the Snowflake SP to enrich top active repos via GitHub REST API.

Fetches: topics, language, description, license, star/fork counts.
Rate-aware: default batch of 500, stays well under 5K/hr auth limit.
"""
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

GHARCHIVE_RAW_DATASET = Dataset("snowflake://VINO_GITHUB_INSIGHTS.RAW.GITHUB_EVENTS")
REPO_METADATA_DATASET = Dataset("snowflake://VINO_GITHUB_INSIGHTS.RAW.REPO_METADATA")

SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="repo_metadata_enrichment",
    schedule=[GHARCHIVE_RAW_DATASET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={
        "owner": "vino",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["github-insights", "enrichment", "github-api"],
    doc_md=__doc__,
)
def repo_metadata_enrichment():
    enrich = SnowflakeOperator(
        task_id="enrich_top_repos",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="CALL VINO_GITHUB_INSIGHTS.RAW.SP_ENRICH_REPO_METADATA(500)",
        outlets=[REPO_METADATA_DATASET],
    )

    enrich


repo_metadata_enrichment()
