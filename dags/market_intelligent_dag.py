"""
AIRFLOW DAG: Market Intelligence Pipeline
-------------------------------------------
Orchestrates: Extract → Upload → Silver → Gold → DuckDB → dbt

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "hamza",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT = "/opt/airflow/project"

with DAG(
    dag_id="market_intelligence_pipeline",
    default_args=default_args,
    description="Extract → Transform → Serve market intelligence data",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 19),
    catchup=False,
    tags=["market-intelligence", "etl", "nlp"],
) as dag:

    # --- EXTRACT (parallel) ---
    extract_stocks = BashOperator(
        task_id="extract_stocks",
        bash_command=f"cd {PROJECT} && python -m ingestion.extract_stocks",
    )

    extract_news = BashOperator(
        task_id="extract_news",
        bash_command=f"cd {PROJECT} && python -m ingestion.extract_news",
    )

    extract_fred = BashOperator(
        task_id="extract_fred",
        bash_command=f"cd {PROJECT} && python -m ingestion.extract_fred",
    )

    # --- UPLOAD TO AZURE ---
    upload_to_lake = BashOperator(
        task_id="upload_to_lake",
        bash_command=f"cd {PROJECT} && python -m azure.upload_to_lake",
    )

    # --- TRANSFORM ---
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"cd {PROJECT} && python -m spark.bronze_to_silver",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"cd {PROJECT} && python -m spark.silver_to_gold",
    )

    # --- LOAD ---
    load_duckdb = BashOperator(
        task_id="load_to_duckdb",
        bash_command=f"cd {PROJECT} && python -m spark.load_to_duckdb",
    )

    # --- DBT ---
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT}/dbt && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT}/dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT}/dbt && dbt test",
    )

    # --- DAG DEPENDENCIES ---
    # Extract in parallel, then sequential
    [extract_stocks, extract_news, extract_fred] >> upload_to_lake
    upload_to_lake >> bronze_to_silver >> silver_to_gold >> load_duckdb
    load_duckdb >> dbt_deps >> dbt_run >> dbt_test