# github-insight-machine

A real-time GitHub ecosystem intelligence pipeline built on GH Archive, Snowflake, Airflow, and dbt. Ingests all public GitHub events hourly, enriches them with GitHub API metadata, and produces analytics-ready mart tables for trending repos, contributor patterns, community health, and ecosystem signals.

---

## What it does

- Ingests public GitHub events (stars, pushes, forks, PRs, issues, releases, creates) from [GH Archive](https://www.gharchive.org/) every hour
- Enriches active repos with metadata from the GitHub REST API: topics, language, description, license, star/fork counts
- Transforms raw events into mart tables via dbt (staging → intermediate → marts)
- Surfaces insights on: trending repos, viral velocity candidates, community health, org OSS investment, cross-org contributor migration

---

## Architecture

```
GH Archive (hourly JSON) 
    ↓ Airflow DAG: gharchive_ingest_hourly
    ↓ Snowflake SP: SP_INGEST_GHARCHIVE_HOUR
RAW.GITHUB_EVENTS
    ↓ (Airflow Dataset trigger)
    ├── Airflow DAG: repo_metadata_enrichment → RAW.REPO_METADATA
    └── Airflow DAG: dbt_github_insights (Cosmos)
            ↓
        STAGING (views)      → stg_watch/push/fork/pr/issue/release/create_events
        INTERMEDIATE (tables) → int_repo_daily_activity, int_contributor_profiles, int_org_activity
        MARTS (tables)        → 6 mart tables (see below)
```

---

## Mart inventory

| Mart | What it answers | Time window |
|---|---|---|
| `mart_trending_repos` | Daily ranked repos by composite activity score (stars, pushes, forks, PRs, issues) | Last 30 days |
| `mart_trending_by_topic` | Trending repos enriched with language, topics, description from GitHub API | Last 30 days |
| `mart_viral_velocity` | Repos with sudden star acceleration vs their own 7-day baseline — velocity_class: explosive / viral / accelerating | Last 15 days |
| `mart_new_repo_breakout` | Repos < 30 days old with high early traction — breakout_tier: instant_hit / fast_mover / rising | Last 30 days |
| `mart_release_momentum` | Projects cutting releases with sustained PR/push activity — project_stage: active_oss / growing / early_release | Last 90 days |
| `mart_ai_ecosystem` | Trending AI/LLM/MCP/agent repos with week-over-week momentum — momentum: surging / growing / new_to_radar | Last 30 days |
| `mart_ecosystem_hourly` | Macro GitHub activity by event type — is OSS accelerating? | Last 7 days |
| `mart_fork_conversion` | Fork-to-PR conversion rate + community health: thriving / growing / fork_graveyard | Last 90 days |
| `mart_org_scoreboard` | Which orgs are most active in OSS — push ratio, PR ratio, contributors per repo | Last 30 days |
| `mart_contributor_migration` | Developers active across multiple orgs — talent flow and cross-pollination signals | Last 90 days |

---

## DAGs

| DAG | Schedule | Trigger |
|---|---|---|
| `gharchive_ingest_hourly` | `@hourly` | Time-based |
| `repo_metadata_enrichment` | Dataset-driven | After `gharchive_ingest_hourly` |
| `dbt_github_insights` | Dataset-driven | After `gharchive_ingest_hourly` |
| `gharchive_backfill` | Manual only | Triggered with `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}` |

All DAGs communicate via an Airflow Dataset: `snowflake://VINO_GITHUB_INSIGHTS.RAW.GITHUB_EVENTS`

---

## Local setup

**Prerequisites:** Docker, [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)

```bash
# Start Airflow locally
astro dev start

# UI: http://localhost:8080 (admin/admin)
```

**Required Airflow connection** (`snowflake_default`):

| Field | Value |
|---|---|
| Conn type | Snowflake |
| Account | `<your-account>` |
| Login | `<your-user>` |
| Password | `<your-password>` |
| Schema | `RAW` |
| Database | `VINO_GITHUB_INSIGHTS` |
| Warehouse | `COMPUTE_WH` (or set `SNOWFLAKE_WAREHOUSE`) |

**Required env vars** (set in Airflow or `.env`):

```bash
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN        # optional, defaults to ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH    # optional, defaults to COMPUTE_WH
```

---

## dbt setup

```bash
cd dbt/github_insights

# Install deps
dbt deps

# Compile to verify
dbt compile

# Run all models
dbt run
```

dbt reads credentials from env vars (see `profiles.yml`). No hardcoded credentials.

---

## Snowflake prerequisites

The pipeline expects the following objects in `VINO_GITHUB_INSIGHTS`:

- `RAW.GITHUB_EVENTS` — target table for GH Archive events
- `RAW.REPO_METADATA` — target table for GitHub API enrichment
- `RAW.INGESTION_LOG` — tracks which hourly files have been loaded
- `RAW.SP_INGEST_GHARCHIVE_HOUR(hour_key STRING)` — stored procedure that fetches and loads one GH Archive hour
- `RAW.SP_ENRICH_REPO_METADATA(batch_size INT)` — stored procedure that enriches top N repos via GitHub REST API

