"""
Daily pipeline: extract CSVs → load to warehouse → dbt build → freshness check.

Schedule: 06:00 UTC daily.
Owner: data-engineering@hugoflavio1805
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DATA_DIR = "/opt/airflow/data/raw"


def extract_csvs(**_):
    """In production, this fetches from S3/landing bucket. Locally a no-op."""
    expected = ["customers.csv", "subscriptions.csv", "feedbacks.csv"]
    for name in expected:
        assert (Path(DATA_DIR) / name).exists(), f"Missing source file: {name}"


def load_to_warehouse(**_):
    """Loads CSVs to RAW.* tables. Idempotent: TRUNCATE + COPY in prod (Snowflake)."""
    import duckdb  # local target
    con = duckdb.connect("/opt/airflow/warehouse.duckdb")
    for name in ["customers", "subscriptions", "feedbacks"]:
        con.execute(f"DROP TABLE IF EXISTS raw_{name}")
        con.execute(f"""
            CREATE TABLE raw_{name} AS
            SELECT *, current_timestamp AS _loaded_at
            FROM read_csv_auto('{DATA_DIR}/{name}.csv', header=true)
        """)
    con.close()


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="feedback_revenue_pipeline",
    default_args=default_args,
    description="Daily refresh of feedback × revenue marts",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "dbt", "feedback"],
) as dag:

    extract = PythonOperator(
        task_id="extract_csvs",
        python_callable=extract_csvs,
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_to_warehouse,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR} --target duckdb",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target duckdb",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {DBT_PROFILES_DIR} --target duckdb || true",
    )

    extract >> load >> dbt_deps >> dbt_run >> dbt_test >> dbt_freshness
