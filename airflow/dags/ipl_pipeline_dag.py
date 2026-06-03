# airflow/dags/ipl_pipeline_dag.py

import sys
import os
import requests as http_requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Make ingestion module importable inside the Airflow container
sys.path.insert(0, '/usr/local/airflow')

from ingestion.download_cricsheet import download_from_cricsheet
from ingestion.parse_cricsheet import parse_all_files
from ingestion.upload_to_blob import upload_all_files
from ingestion.load_to_snowflake import (
    get_snowflake_connection,
    create_raw_tables,
    load_matches,
    load_deliveries,
)

# ── Constants ─────────────────────────────────────────────────────────────────
DATA_FOLDER    = os.getenv("DATA_FOLDER", "data")
CONTAINER      = os.getenv("AZURE_CONTAINER_NAME", "raw")
DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")
DBT_JOB_ID     = os.getenv("DBT_JOB_ID")
DBT_API_TOKEN  = os.getenv("DBT_API_TOKEN")

# ── Callable wrappers ─────────────────────────────────────────────────────────
def run_download():
    """Download latest IPL JSON files from Cricsheet automatically."""
    download_from_cricsheet(DATA_FOLDER)

def run_parse():
    """Parse all JSON files into structured match and delivery records."""
    parse_all_files(DATA_FOLDER)

def run_upload():
    """Upload new JSON files to Azure Blob Storage — skips existing files."""
    upload_all_files(DATA_FOLDER, CONTAINER)

def run_load():
    """Load parsed data into Snowflake raw tables."""
    conn = get_snowflake_connection()
    matches, deliveries = parse_all_files(DATA_FOLDER)
    create_raw_tables(conn)
    load_matches(conn, matches)
    load_deliveries(conn, deliveries)
    conn.close()

def trigger_dbt_cloud():
    """Trigger dbt Cloud job via API to run staging and mart models."""
    url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    headers = {"Authorization": f"Token {DBT_API_TOKEN}"}
    payload = {"cause": "Triggered by Airflow — IPL pipeline"}

    response = http_requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    run_id = response.json()["data"]["id"]
    print(f"dbt Cloud job triggered — run ID: {run_id}")
    return run_id

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
    description="End-to-end IPL ELT: Cricsheet → Blob → Snowflake → dbt",
    schedule="0 4 * * *",   # 6am Sweden time (CEST = UTC+2)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ipl", "cricket", "pipeline"],
) as dag:

    download_cricsheet = PythonOperator(
        task_id="download_cricsheet",
        python_callable=run_download,
    )

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

    run_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=trigger_dbt_cloud,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    download_cricsheet >> parse_json >> upload_to_blob >> load_to_snowflake >> run_dbt