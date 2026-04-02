FROM quay.io/astronomer/astro-runtime:13.6.0

# dbt project lives inside the container
ENV DBT_PROJECT_DIR=/usr/local/airflow/dbt/github_insights
