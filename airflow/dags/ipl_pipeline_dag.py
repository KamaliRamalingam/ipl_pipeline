# airflow/dags/ipl_pipeline_dag.py

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# Make ingestion module importable inside the Airflow container
sys.path.insert(0, '/usr/local/airflow')

from ingestion.parse_cricsheet import parse_all_files
from ingestion.upload_to_blob import upload_all_files
from ingestion.load_to_snowflake import (
    get_snowflake_connection,
    create_raw_tables,
    load_matches,
    load_deliveries,
)

# ── Constants ────────────────────────────────────────────────────────────────
DATA_FOLDER = os.getenv("DATA_FOLDER", "data")
CONTAINER   = os.getenv("AZURE_CONTAINER_NAME", "raw")
DBT_DIR     = "/usr/local/airflow/dbt"

# ── Callable wrappers for PythonOperator ─────────────────────────────────────
def run_parse():
    parse_all_files(DATA_FOLDER)

def run_upload():
    upload_all_files(DATA_FOLDER, CONTAINER)

def run_load():
    conn = get_snowflake_connection()
    matches, deliveries = parse_all_files(DATA_FOLDER)
    create_raw_tables(conn)
    load_matches(conn, matches)
    load_deliveries(conn, deliveries)
    conn.close()

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "kamali",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ipl_pipeline",
    description="End-to-end IPL ELT: parse → Blob → Snowflake → dbt",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ipl", "cricket", "pipeline"],
) as dag:

    parse_json = PythonOperator(
        task_id="parse_json",
        python_callable=run_parse,
    )

    upload_to_blob = PythonOperator(
        task_id="upload_to_blob",
        python_callable=run_upload,
    )

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=run_load,
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    parse_json >> upload_to_blob >> load_to_snowflake >> run_dbt >> test_dbt
