# IPL Analytics Pipeline

End-to-end data engineering pipeline for IPL cricket analytics (2008–2026).

## Stack
- **Source**: Cricsheet IPL JSON (1,241 matches)
- **Ingestion**: Python → Azure Blob Storage
- **Warehouse**: Snowflake
- **Transformation**: dbt (staging → dims → facts → marts)
- **Orchestration**: Apache Airflow (Astro CLI)
- **Dashboard**: Tableau Desktop

## Project Structure
- `ingestion/` — Python scripts to parse and upload data
- `dbt/` — transformation models, tests, documentation
- `airflow/` — DAGs and orchestration
- `config/` — non-secret configuration
- `tests/` — unit tests
- `docs/` — architecture decisions and diagrams

## Setup
1. Copy `.env.example` to `.env` and fill in credentials
2. Activate virtualenv: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Configure dbt: edit `dbt/profiles.yml`
5. Start Airflow: `astro dev start` from `airflow/`

## Pipeline Flow
1. Python parses Cricsheet JSON → uploads raw files to Azure Blob
2. Python loads flattened data into Snowflake raw tables
3. dbt transforms raw → staging → dims → facts → marts
4. Airflow DAG orchestrates and schedules the full pipeline
5. Tableau connects to Snowflake marts for dashboards
