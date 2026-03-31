from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "nalluraj",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="job_market_pipeline",
    default_args=default_args,
    description="ETL pipeline: API -> S3 -> MySQL -> Snowflake -> dbt",
    schedule_interval="0 6 * * *",  # Every day at 6 AM
    start_date=datetime(2026, 3, 30),
    catchup=False,
) as dag:

    fetch_jobs = BashOperator(
        task_id="fetch_jobs",
        bash_command="cd /opt/airflow && python ingestion/job_fetcher.py",
    )

    extract_skills = BashOperator(
        task_id="extract_skills",
        bash_command="cd /opt/airflow && python ingestion/skill_extractor.py",
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command="cd /opt/airflow && python ingestion/snowflake_loader.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/transformation/job_market_dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/transformation/job_market_dbt && dbt test",
    )

    # Order: fetch -> extract skills -> load snowflake -> dbt run -> dbt test
    fetch_jobs >> extract_skills >> load_snowflake >> dbt_run >> dbt_test
