"""
DAG 2: dbt Transform Pipeline
==============================
Triggered by the Airflow Dataset produced by the ingestion DAG.
Runs dbt models using Cosmos (DbtTaskGroup) so each model is
an individual Airflow task with retries and lineage.

Flow: staging views → intermediate tables → mart tables
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag
from airflow.datasets import Dataset
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Same dataset produced by the ingestion DAG
GHARCHIVE_RAW_DATASET = Dataset("snowflake://VINO_GITHUB_INSIGHTS.RAW.GITHUB_EVENTS")

DBT_PROJECT_PATH = Path(os.environ.get(
    "DBT_PROJECT_DIR",
    "/usr/local/airflow/dbt/github_insights"
))

SNOWFLAKE_CONN_ID = "snowflake_default"


@dag(
    dag_id="dbt_github_insights",
    # Triggered when the ingestion DAG updates the dataset
    schedule=[GHARCHIVE_RAW_DATASET],
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args={
        "owner": "vino",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["github-insights", "dbt", "transform"],
    doc_md=__doc__,
)
def dbt_github_insights():
    profile_config = ProfileConfig(
        profile_name="github_insights",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=SNOWFLAKE_CONN_ID,
            profile_args={
                "database": "VINO_GITHUB_INSIGHTS",
                "schema": "RAW",
                "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            },
        ),
    )

    dbt_transforms = DbtTaskGroup(
        group_id="dbt_transforms",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        render_config=RenderConfig(
            # Run all models in dependency order
            select=["path:models"],
        ),
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=1),
        },
    )

    dbt_transforms


dbt_github_insights()
