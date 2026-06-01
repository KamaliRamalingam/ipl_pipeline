# IPL Analytics Pipeline

End-to-end data engineering pipeline for IPL cricket analytics (2008–2026).

## Stack
- **Source**: Cricsheet IPL JSON files (1,241 matches, 2008–2026)
- **Ingestion**: Python → Azure Blob Storage
- **Warehouse**: Snowflake
- **Transformation**: dbt (staging → dimensions → facts → marts)
- **Orchestration**: Apache Airflow (Astro CLI)
- **Dashboard**: Tableau Desktop
- **Language**: Python 3.11 + SQL

## Project Structure
```
ipl_pipeline/
├── ingestion/
│   ├── __init__.py
│   ├── utils.py
│   ├── parse_cricsheet.py
│   ├── upload_to_blob.py
│   └── load_to_snowflake.py
├── dbt/
│   └── models/
│       ├── staging/
│       ├── dimensions/
│       ├── facts/
│       └── marts/
├── airflow/
│   └── dags/
│       └── ipl_pipeline_dag.py
├── config/
│   └── pipeline_config.yml
├── tests/
│   ├── test_parse_cricsheet.py
│   └── test_upload.py
├── data/                        ← Cricsheet JSON files (gitignored)
├── docs/
│   └── architecture.md
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Environment Variables
Create a `.env` file with these values (never commit this file):
```
# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_CONTAINER_NAME=raw

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=IPL_DB
SNOWFLAKE_WAREHOUSE=IPL_WH
SNOWFLAKE_ROLE=DBT_ROLE

# Pipeline
DATA_FOLDER=data
```

## Pipeline Flow
```
data/ (Cricsheet JSON files)
    → ingestion/parse_cricsheet.py   (parse + flatten JSON)
    → ingestion/upload_to_blob.py    (upload raw JSON to Azure Blob)
    → ingestion/load_to_snowflake.py (load flattened data to Snowflake RAW schema)
    → dbt run                        (staging → dimensions → facts → marts)
    → Airflow DAG                    (orchestrate and schedule)
    → Tableau Desktop                (connect to Snowflake marts)
```

---

## Claude Code Build Instructions
> These are step-by-step instructions for Claude Code to build the pipeline.
> Follow them in order. Do not skip steps.

---

### STEP 1 — Write ingestion/utils.py

Write `ingestion/utils.py` with the following:

1. A `get_logger(name)` function that returns a Python logger with:
   - StreamHandler writing to console
   - Format: `%(asctime)s - %(name)s - %(levelname)s - %(message)s`
   - Log level: INFO

2. A `load_env()` function that:
   - Loads `.env` file using `python-dotenv`
   - Returns a dict with all required env vars
   - Raises a clear error if any required variable is missing
   - Required vars: AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME, SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE, DATA_FOLDER

3. A `retry(max_attempts=3, delay=2)` decorator that:
   - Retries a function up to max_attempts times on exception
   - Waits delay seconds between retries
   - Logs each retry attempt
   - Raises the last exception if all attempts fail

Add docstrings to all functions.

---

### STEP 2 — Write ingestion/parse_cricsheet.py

The Cricsheet JSON structure is:
- Top level keys: `meta`, `info`, `innings`
- `info` contains: `dates`, `season`, `teams`, `venue`, `city`, `toss`, `outcome`, `player_of_match`, `event`
- `innings` is a list of 2 innings, each with: `team`, `overs`
- Each over has: `over` (number), `deliveries` (list)
- Each delivery has: `batter`, `bowler`, `non_striker`, `runs`, optional `wickets`, optional `extras`
- `season` format varies: `"2007/08"` for early years, `"2026"` for recent years
- All optional fields must use `.get()` to avoid KeyError

Write `ingestion/parse_cricsheet.py` with:

1. `parse_match_info(file_path, match_id)` → returns a dict:
   - match_id, match_date (from dates[0]), season, team1 (teams[0]), team2 (teams[1])
   - venue, city, toss_winner, toss_decision
   - match_winner (from outcome.winner), win_by_runs (from outcome.by.runs), win_by_wickets (from outcome.by.wickets)
   - player_of_match (first value or None), event_name, event_stage, event_match_number
   - Use .get() for all optional fields

2. `parse_deliveries(file_path, match_id)` → returns a list of dicts, one per delivery:
   - match_id, inning_number (1 or 2), batting_team
   - over_number, ball_number (position in over, 0-indexed)
   - batter, bowler, non_striker
   - runs_batter, runs_extras, runs_total
   - extras_type (first key found in extras dict, or None)
   - is_wicket (True if wickets key exists), wicket_kind, player_out, fielder (first fielder name or None)
   - Use .get() for all optional fields

3. `parse_all_files(data_folder)` → main function:
   - Takes folder path as string
   - Gets match_id from filename (stem without extension)
   - Loops through all .json files in folder
   - Calls parse_match_info and parse_deliveries for each file
   - Collects results into all_matches (list) and all_deliveries (list)
   - Logs progress every 100 files: "Processed 100/1241 files"
   - Wraps each file in try/except, logs filename on error, continues
   - Returns all_matches, all_deliveries

4. A `if __name__ == "__main__":` block that:
   - Loads DATA_FOLDER from environment
   - Calls parse_all_files()
   - Prints total matches and total deliveries parsed

Import utils.py for logging. Add docstrings to all functions.

---

### STEP 3 — Write ingestion/upload_to_blob.py

Write `ingestion/upload_to_blob.py` with:

1. `get_blob_client()` → returns an Azure BlobServiceClient:
   - Reads AZURE_STORAGE_CONNECTION_STRING from environment
   - Uses BlobServiceClient.from_connection_string()

2. `upload_file(file_path, container_name, blob_path)` → uploads one file:
   - Checks if blob already exists — if yes, skip and log "already exists"
   - If no, uploads the file
   - Logs success with filename
   - Uses the @retry decorator from utils.py

3. `upload_all_files(data_folder, container_name)` → main function:
   - Loops through all .json files in data_folder
   - Sets blob_path as `ipl/{filename}` (e.g. `ipl/1082591.json`)
   - Calls upload_file for each
   - Logs progress every 100 files
   - Returns count of uploaded files and count of skipped files

4. A `if __name__ == "__main__":` block that:
   - Loads env vars
   - Calls upload_all_files()
   - Prints upload summary

Import utils.py for logging and retry decorator. Add docstrings to all functions.

---

### STEP 4 — Write ingestion/load_to_snowflake.py

Write `ingestion/load_to_snowflake.py` with:

1. `get_snowflake_connection()` → returns a Snowflake connection:
   - Reads all Snowflake env vars
   - Uses snowflake.connector.connect()

2. `create_raw_tables(conn)` → creates tables if not exist:
   - Creates `RAW.MATCHES` with columns: match_id, match_date, season, team1, team2, venue, city, toss_winner, toss_decision, match_winner, win_by_runs, win_by_wickets, player_of_match, event_name, event_stage, event_match_number, loaded_at (TIMESTAMP default CURRENT_TIMESTAMP)
   - Creates `RAW.DELIVERIES` with columns: match_id, inning_number, batting_team, over_number, ball_number, batter, bowler, non_striker, runs_batter, runs_extras, runs_total, extras_type, is_wicket, wicket_kind, player_out, fielder, loaded_at (TIMESTAMP default CURRENT_TIMESTAMP)
   - Uses CREATE TABLE IF NOT EXISTS

3. `load_matches(conn, matches)` → bulk inserts match records:
   - Checks existing match_ids first to avoid duplicates
   - Only inserts new matches
   - Uses executemany for batch insert
   - Logs count of inserted vs skipped

4. `load_deliveries(conn, deliveries)` → bulk inserts delivery records:
   - Deletes existing deliveries for the match_ids being loaded (clean reload per match)
   - Uses executemany for batch insert
   - Logs count inserted

5. `if __name__ == "__main__":` block that:
   - Calls parse_all_files() from parse_cricsheet.py
   - Gets Snowflake connection
   - Creates tables
   - Loads matches then deliveries
   - Prints summary

Import utils.py for logging. Add docstrings to all functions.

---

### STEP 5 — Write config/pipeline_config.yml

Write `config/pipeline_config.yml` with non-secret configuration:

```yaml
pipeline:
  name: ipl_analytics_pipeline
  data_folder: data
  file_pattern: "*.json"
  log_interval: 100

azure:
  container_name: raw
  blob_prefix: ipl

snowflake:
  database: IPL_DB
  warehouse: IPL_WH
  raw_schema: RAW
  staging_schema: STAGING
  marts_schema: MARTS

dbt:
  project_dir: dbt
  profiles_dir: dbt
  target: dev
```

---

### STEP 6 — Write dbt/models/staging/stg_matches.sql

Write a dbt staging model that:
- Sources from RAW.MATCHES
- Renames and casts all columns cleanly
- Casts match_date to DATE
- Casts win_by_runs and win_by_wickets to INTEGER
- Adds a derived column: `result_type` = 'runs' if win_by_runs is not null, 'wickets' if win_by_wickets is not null, else 'tie/no result'
- Normalises season: replace '2007/08' → '2008', '2009/10' → '2010' etc using a CASE statement
- Materialised as view

---

### STEP 7 — Write dbt/models/staging/stg_deliveries.sql

Write a dbt staging model that:
- Sources from RAW.DELIVERIES
- Renames and casts all columns cleanly
- Casts over_number, ball_number, runs_batter, runs_extras, runs_total to INTEGER
- Casts is_wicket to BOOLEAN
- Adds derived column: `is_boundary` = TRUE if runs_batter >= 4
- Adds derived column: `is_six` = TRUE if runs_batter = 6
- Adds derived column: `is_four` = TRUE if runs_batter = 4
- Materialised as view

---

### STEP 8 — Write dbt/models/dimensions/dim_teams.sql

Write a dbt dimension model that:
- Sources from stg_matches
- Gets all unique team names (UNION of team1 and team2)
- Adds team_id using ROW_NUMBER()
- Materialised as table

---

### STEP 9 — Write dbt/models/dimensions/dim_venues.sql

Write a dbt dimension model that:
- Sources from stg_matches
- Gets all unique venues with their city
- Adds venue_id using ROW_NUMBER()
- Materialised as table

---

### STEP 10 — Write dbt/models/dimensions/dim_players.sql

Write a dbt dimension model that:
- Sources from stg_deliveries
- Gets all unique player names from batter, bowler columns (UNION)
- Adds player_id using ROW_NUMBER()
- Materialised as table

---

### STEP 11 — Write dbt/models/facts/fct_match_results.sql

Write a dbt fact model that:
- Sources from stg_matches
- Joins with dim_teams for team1_id, team2_id, winner_id
- Joins with dim_venues for venue_id
- Includes all match-level metrics
- Materialised as table
- Add unique and not_null tests in schema.yml for match_id

---

### STEP 12 — Write dbt/models/facts/fct_deliveries.sql

Write a dbt fact model that:
- Sources from stg_deliveries
- Joins with dim_players for batter_id, bowler_id
- Joins with fct_match_results for match context
- Materialised as incremental model using match_id as unique key
- Add not_null tests for match_id, batter, bowler

---

### STEP 13 — Write dbt/models/marts/mart_batting_stats.sql

Write a dbt mart model that aggregates by player and season:
- Total runs, balls faced, innings played
- Strike rate = (runs / balls) * 100
- Average = runs / innings
- Boundary count (fours + sixes)
- Highest score per season
- Materialised as table

---

### STEP 14 — Write dbt/models/marts/mart_bowling_stats.sql

Write a dbt mart model that aggregates by player and season:
- Total wickets, overs bowled, runs conceded
- Economy rate = runs / overs
- Average = runs / wickets
- Strike rate = balls / wickets
- Best bowling figures in a single match
- Materialised as table

---

### STEP 15 — Write dbt/models/marts/mart_team_performance.sql

Write a dbt mart model that aggregates by team and season:
- Matches played, matches won, win rate
- Toss wins, toss win to match win conversion rate
- Average runs scored per match
- Materialised as table

---

### STEP 16 — Write dbt/models/schema.yml

Write a dbt schema.yml file that documents all models with:
- Description for every model
- Description for every column in every model
- Tests: not_null and unique on all primary keys
- Tests: not_null on all foreign keys
- accepted_values test on: result_type, toss_decision, wicket_kind, extras_type

---

### STEP 17 — Write airflow/dags/ipl_pipeline_dag.py

Write an Airflow DAG with:
- dag_id: `ipl_pipeline`
- schedule: `@daily`
- start_date: datetime(2025, 1, 1)
- catchup: False
- tags: ['ipl', 'cricket', 'pipeline']

Tasks in this order:
1. `parse_json` — PythonOperator calling parse_all_files()
2. `upload_to_blob` — PythonOperator calling upload_all_files()
3. `load_to_snowflake` — PythonOperator calling load functions
4. `run_dbt` — BashOperator running `dbt run --project-dir /usr/local/airflow/dbt`
5. `test_dbt` — BashOperator running `dbt test --project-dir /usr/local/airflow/dbt`

Dependencies: parse_json >> upload_to_blob >> load_to_snowflake >> run_dbt >> test_dbt

Add default_args with: owner, retries=2, retry_delay=timedelta(minutes=5), email_on_failure=False

---

### STEP 18 — Write tests/test_parse_cricsheet.py

Write pytest unit tests for parse_cricsheet.py:
- Test parse_match_info with a known JSON fixture → assert specific field values
- Test parse_deliveries returns correct number of rows
- Test parse_deliveries handles missing wickets field gracefully
- Test parse_deliveries handles missing extras field gracefully
- Test parse_all_files returns two non-empty lists
- Use a small sample JSON fixture (2-3 deliveries) created inside the test file

---

### STEP 19 — Write .env.example

Write `.env.example` with all required environment variables as placeholders:
```
# Azure Blob Storage
AZURE_STORAGE_CONNECTION_STRING=your_azure_connection_string_here
AZURE_CONTAINER_NAME=raw

# Snowflake
SNOWFLAKE_ACCOUNT=your_account_here
SNOWFLAKE_USER=your_username_here
SNOWFLAKE_PASSWORD=your_password_here
SNOWFLAKE_DATABASE=IPL_DB
SNOWFLAKE_WAREHOUSE=IPL_WH
SNOWFLAKE_ROLE=DBT_ROLE

# Pipeline
DATA_FOLDER=data
```

---

### STEP 20 — Write requirements.txt

Write `requirements.txt` with pinned versions for all installed packages:
- azure-storage-blob
- snowflake-connector-python
- dbt-snowflake
- apache-airflow
- python-dotenv
- pytest
