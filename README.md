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

---

## Reddit Sentiment Micro-Batch Pipeline

Real-time IPL sentiment analysis from Reddit r/Cricket, running every 10 minutes alongside the existing daily batch pipeline.

### Architecture

```
Reddit r/Cricket (public JSON API, no key)
    → include/reddit/extractor.py       (fetch posts via requests)
    → include/reddit/sentiment.py       (VADER sentiment scoring)
    → include/reddit/blob_uploader.py   (upload raw JSON → Azure Blob)
    → include/reddit/snowflake_loader.py (load scored rows → Snowflake RAW)
    → dbt Cloud (stg_reddit_sentiment → mart_ipl_sentiment)
    → Airflow DAG: reddit_sentiment_dag.py (orchestrate every 10 min)
```

### Why this architecture?

- **Blob as raw landing zone first**: Raw JSON is saved to Azure Blob before any transformation. This preserves the original data unmodified and follows the medallion architecture your course requires (Raw → Staging → Mart). If Snowflake loading fails, the raw data is not lost.
- **Sentiment scored in-flight**: VADER runs inside the Airflow task worker (not in dbt) because it requires Python, not SQL. The scores land in the RAW table so dbt can treat them as source data.
- **Micro-batch not streaming**: Every 10 minutes is micro-batch. True streaming (Kafka, Event Hub) would be over-engineering for Reddit's low volume. Airflow `schedule="*/10 * * * *"` is the correct tool here.
- **Separate DAG from daily pipeline**: Reddit sentiment runs independently. Coupling it to `ipl_pipeline_dag.py` would mean a Reddit API failure could block your cricket stats pipeline. Separation = fault isolation.
- **`requests` not PRAW**: Reddit's public `.json` API requires no authentication for read-only access to public subreddits. Simpler dependency, no OAuth flow needed.

### New Files Added

```
ipl_pipeline/
├── include/
│   └── reddit/
│       ├── __init__.py                         ← makes reddit a Python package
│       ├── extractor.py                        ← fetch posts from Reddit JSON API
│       ├── sentiment.py                        ← VADER scoring logic
│       ├── blob_uploader.py                    ← upload raw JSON to Azure Blob
│       └── snowflake_loader.py                 ← load scored records to Snowflake RAW
├── dags/
│   └── reddit_sentiment_dag.py                 ← Airflow micro-batch DAG
├── snowflake/
│   └── reddit_raw_ddl.sql                      ← run once before first DAG run
└── dbt/
    └── models/
        ├── staging/
        │   └── stg_reddit_sentiment.sql        ← clean + cast raw Reddit rows
        └── marts/
            └── mart_ipl_sentiment.sql          ← aggregated sentiment by team/date
```

### New Environment Variables

Add these to your `.env` file (and `.env.example`):

```
# Reddit Micro-Batch
AZURE_BLOB_REDDIT_PREFIX=reddit/raw
REDDIT_SUBREDDIT=Cricket
REDDIT_SEARCH_QUERY=IPL
REDDIT_MAX_POSTS=25
```

No Reddit API key needed. All existing Snowflake and Azure variables are reused.

### Snowflake Setup (run once)

Before the first DAG run, execute this in Snowflake:

```sql
-- Run: snowflake/reddit_raw_ddl.sql
-- Creates: IPL_DB.RAW.REDDIT_IPL_POSTS
-- This is the raw landing table. dbt reads from here.
```

---

## Claude Code Build Instructions — Reddit Sentiment Micro-Batch

> These steps follow on from Steps 1–20 above.
> All new files go in `include/reddit/` and `dags/`.
> Do not modify any existing ingestion files.
> Follow steps in order. Complete each file fully before moving to the next.

---

### STEP 21 — Create include/reddit/__init__.py

Create an empty `include/reddit/__init__.py` file.
This makes `reddit` a Python package so Airflow can import from it cleanly.

---

### STEP 22 — Write include/reddit/extractor.py

Write `include/reddit/extractor.py` with the following:

**Context**: Reddit exposes public subreddit data as JSON at:
`https://www.reddit.com/r/{subreddit}/search.json?q={query}&sort=new&limit={limit}&t=hour`
No API key is needed. A `User-Agent` header is required or Reddit returns 429.

**Function 1**: `fetch_reddit_posts(subreddit, query, limit, fetched_at) -> list[dict]`

- Parameters:
  - `subreddit`: str, e.g. `"Cricket"`
  - `query`: str, e.g. `"IPL"`
  - `limit`: int, default 25
  - `fetched_at`: datetime, the timestamp to record when we fetched (passed in, not generated here — makes testing easier)
- Build URL: `https://www.reddit.com/r/{subreddit}/search.json`
- Query params: `q=query, sort=new, limit=limit, t=hour`
- Headers: `{"User-Agent": "ipl-sentiment-pipeline/1.0 (educational project)"}`
- Use `requests.get()` with `timeout=15`
- Raise `requests.HTTPError` if response status is not 200 using `response.raise_for_status()`
- Parse JSON response: `data["data"]["children"]` is the list of posts
- For each post, extract from `post["data"]`:
  - `post_id` = `id`
  - `post_title` = `title`
  - `post_author` = `author`
  - `subreddit` = `subreddit`
  - `post_url` = `url`
  - `post_body` = `selftext` (can be empty string — keep as-is)
  - `score` = `score`
  - `upvote_ratio` = `upvote_ratio`
  - `num_comments` = `num_comments`
  - `created_utc` = convert `created_utc` (Unix epoch float) to UTC datetime using `datetime.utcfromtimestamp()`
  - `fetched_at` = the passed-in `fetched_at` parameter
- Return list of these dicts
- If response is empty or `data["data"]["children"]` is missing, return empty list
- Log: number of posts fetched

**Function 2**: `get_text_for_sentiment(post: dict) -> str`

- Takes a single post dict from above
- Returns: `post_title + " " + post_body` stripped of whitespace
- If both are empty, return empty string
- This is the text we will pass to VADER

Import `get_logger` from `ingestion.utils`. Add docstrings. No PRAW. No API key.

---

### STEP 23 — Write include/reddit/sentiment.py

Write `include/reddit/sentiment.py` with the following:

**Context**: VADER (Valence Aware Dictionary and sEntiment Reasoner) is a rule-based sentiment analyser built for social media text. It returns four scores: `pos`, `neg`, `neu`, `compound`. The `compound` score ranges from -1.0 (most negative) to +1.0 (most positive). Standard thresholds: compound >= 0.05 → POSITIVE, compound <= -0.05 → NEGATIVE, else NEUTRAL.

**Function 1**: `get_sentiment_analyser() -> SentimentIntensityAnalyzer`

- Imports `SentimentIntensityAnalyzer` from `vaderSentiment.vaderSentiment`
- Returns a single instance
- Log: "VADER sentiment analyser initialised"

**Function 2**: `score_post(post: dict, analyser: SentimentIntensityAnalyzer) -> dict`

- Takes a post dict (as returned by extractor.py) and the analyser instance
- Gets the text via `get_text_for_sentiment(post)` — import this from extractor.py
- If text is empty string: set all scores to None, label to "NEUTRAL"
- Else: call `analyser.polarity_scores(text)` which returns `{"pos": float, "neg": float, "neu": float, "compound": float}`
- Determine `sentiment_label`:
  - compound >= 0.05 → "POSITIVE"
  - compound <= -0.05 → "NEGATIVE"
  - else → "NEUTRAL"
- Return the original post dict with four new keys added:
  - `sentiment_compound` = compound score
  - `sentiment_positive` = pos score
  - `sentiment_negative` = neg score
  - `sentiment_neutral` = neu score
  - `sentiment_label` = label string

**Function 3**: `score_all_posts(posts: list[dict], analyser: SentimentIntensityAnalyzer) -> list[dict]`

- Calls `score_post()` for each post in the list
- Returns list of scored dicts
- Log: f"Scored sentiment for {n} posts"

Import `get_logger` from `ingestion.utils`. Import `get_text_for_sentiment` from `include.reddit.extractor`. Add docstrings.

---

### STEP 24 — Write include/reddit/blob_uploader.py

Write `include/reddit/blob_uploader.py` with the following:

**Context**: We upload the raw list of posts (as JSON) to Azure Blob Storage before loading to Snowflake. This is the raw landing zone. The file path encodes the timestamp so each micro-batch is a separate file. This follows the same pattern as the existing `ingestion/upload_to_blob.py` but is scoped to Reddit data.

**Function 1**: `upload_reddit_batch(posts: list[dict], fetched_at: datetime, container_name: str, blob_prefix: str) -> str`

- Parameters:
  - `posts`: the list of scored post dicts
  - `fetched_at`: datetime of the fetch (used to build the blob path)
  - `container_name`: Azure container name (from env)
  - `blob_prefix`: prefix like `"reddit/raw"` (from env)
- Build blob path as:
  `{blob_prefix}/{fetched_at.strftime('%Y/%m/%d/%H')}/reddit_ipl_{fetched_at.strftime('%Y%m%d_%H%M%S')}.json`
  Example: `reddit/raw/2026/06/06/14/reddit_ipl_20260606_143000.json`
- Serialise posts to JSON string using `json.dumps(posts, default=str, indent=2)`
  - `default=str` handles datetime serialisation automatically
- Get Azure BlobServiceClient using `BlobServiceClient.from_connection_string()` with `AZURE_STORAGE_CONNECTION_STRING` from environment
- Upload using `blob_client.upload_blob(data, overwrite=True)`
  - `overwrite=True` because if the DAG retries at the same minute, we want to replace not error
- Log: blob path on success
- Return the blob path string (we store this in Snowflake for traceability)
- Use the `@retry` decorator from `ingestion.utils`

Import `get_logger` and `retry` from `ingestion.utils`. Import `os` for env vars. Add docstrings.

---

### STEP 25 — Write include/reddit/snowflake_loader.py

Write `include/reddit/snowflake_loader.py` with the following:

**Context**: This loads the scored Reddit posts into `IPL_DB.RAW.REDDIT_IPL_POSTS`. The table schema is defined in `snowflake/reddit_raw_ddl.sql` and must already exist. We use `snowflake-connector-python` directly (same as the existing `ingestion/load_to_snowflake.py`). Each row gets a `pipeline_load_id` (UUID for the batch) so we can trace which DAG run inserted it.

**Function 1**: `get_snowflake_connection()`

- Reads env vars: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
- Connects to Snowflake using `snowflake.connector.connect()`
- Sets `schema="RAW"` explicitly
- Returns connection object

**Function 2**: `load_reddit_posts(posts: list[dict], blob_path: str, load_id: str) -> int`

- Parameters:
  - `posts`: list of scored post dicts (each dict has all fields from extractor + sentiment)
  - `blob_path`: the Azure Blob path string returned by `upload_reddit_batch()` — stored for traceability
  - `load_id`: a UUID string for this DAG run batch (generated once in the DAG, passed in)
- Opens Snowflake connection
- Builds list of tuples matching the INSERT column order exactly:
  ```
  (load_id, current_utc_timestamp, blob_path,
   post_id, post_title, post_author, subreddit, post_url, post_body,
   score, upvote_ratio, num_comments,
   created_utc, fetched_at,
   sentiment_compound, sentiment_positive, sentiment_negative, sentiment_neutral, sentiment_label,
   json.dumps(original_post_dict, default=str))
  ```
- For the `RAW_JSON` column, pass the entire post dict serialised as a JSON string
- Use `cursor.executemany()` with this INSERT statement:
  ```sql
  INSERT INTO IPL_DB.RAW.REDDIT_IPL_POSTS (
      PIPELINE_LOAD_ID, PIPELINE_LOADED_AT, PIPELINE_BATCH_FILE,
      POST_ID, POST_TITLE, POST_AUTHOR, SUBREDDIT, POST_URL, POST_BODY,
      SCORE, UPVOTE_RATIO, NUM_COMMENTS,
      CREATED_UTC, FETCHED_AT,
      SENTIMENT_COMPOUND, SENTIMENT_POSITIVE, SENTIMENT_NEGATIVE, SENTIMENT_NEUTRAL, SENTIMENT_LABEL,
      RAW_JSON
  ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
  ```
  Note: `RAW_JSON` is a VARIANT column in Snowflake — it requires `PARSE_JSON(%s)` not `%s`
- Commit after executemany
- Close connection
- Log: f"Loaded {n} Reddit posts to Snowflake RAW.REDDIT_IPL_POSTS (batch: {load_id})"
- Return count of rows inserted
- Wrap in try/except, log error, re-raise on failure

Import `get_logger` from `ingestion.utils`. Add docstrings.

---

### STEP 26 — Write dags/reddit_sentiment_dag.py

Write `dags/reddit_sentiment_dag.py` — the Airflow micro-batch DAG.

**Use the modern `@dag` / `@task` decorator style** (as taught in the course, not the old `PythonOperator` style).

**DAG configuration**:
```python
dag_id = "reddit_sentiment_pipeline"
schedule = "*/10 * * * *"    # every 10 minutes
start_date = datetime(2026, 1, 1)
catchup = False
max_active_runs = 1           # prevent overlapping runs
tags = ["reddit", "sentiment", "ipl", "micro-batch"]
default_args = {
    "owner": "kamali",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}
```

**Explanation of `max_active_runs = 1`**: If a 10-minute run takes longer than 10 minutes (e.g. Snowflake is slow), Airflow will not start a second overlapping run. This prevents duplicate loads.

**Tasks** — use `@task` decorator for all four:

**Task 1: `extract_posts`**
```python
@task()
def extract_posts() -> list[dict]:
```
- Imports: `from include.reddit.extractor import fetch_reddit_posts`
- Reads from env: REDDIT_SUBREDDIT (default "Cricket"), REDDIT_SEARCH_QUERY (default "IPL"), REDDIT_MAX_POSTS (default 25, cast to int)
- Gets `fetched_at = datetime.utcnow()`
- Calls `fetch_reddit_posts(subreddit, query, limit, fetched_at)`
- If result is empty list: log warning "No posts fetched — Reddit API may be rate limiting" and return empty list (do not raise — empty result is valid)
- Returns list of post dicts
- Note: Airflow XCom serialises the return value as JSON between tasks. datetime objects in the dicts must be converted to ISO strings before returning: loop through posts and convert any datetime values using `.isoformat()`

**Task 2: `score_sentiment`**
```python
@task()
def score_sentiment(posts: list[dict]) -> list[dict]:
```
- Imports: `from include.reddit.sentiment import get_sentiment_analyser, score_all_posts`
- If `posts` is empty: log "No posts to score, skipping" and return empty list
- Initialises analyser: `analyser = get_sentiment_analyser()`
- Calls `score_all_posts(posts, analyser)`
- Returns scored posts list

**Task 3: `upload_to_blob`**
```python
@task()
def upload_to_blob(scored_posts: list[dict]) -> str:
```
- Imports: `from include.reddit.blob_uploader import upload_reddit_batch`
- If `scored_posts` is empty: log "No posts to upload, skipping" and return ""
- Reads from env: AZURE_CONTAINER_NAME, AZURE_BLOB_REDDIT_PREFIX (default "reddit/raw")
- Reconstructs `fetched_at` datetime from the first post's `fetched_at` ISO string (convert back with `datetime.fromisoformat()`)
- Calls `upload_reddit_batch(scored_posts, fetched_at, container_name, blob_prefix)`
- Returns blob path string

**Task 4: `load_to_snowflake`**
```python
@task()
def load_to_snowflake(scored_posts: list[dict], blob_path: str) -> None:
```
- Imports: `from include.reddit.snowflake_loader import load_reddit_posts`
- Imports: `import uuid`
- If `scored_posts` is empty: log "No posts to load, skipping" and return
- Generates `load_id = str(uuid.uuid4())`
- Calls `load_reddit_posts(scored_posts, blob_path, load_id)`
- Logs: f"Micro-batch complete. load_id={load_id}, posts={len(scored_posts)}, blob={blob_path}"

**DAG wiring** — use the functional style with data passing between tasks:
```python
@dag(...)
def reddit_sentiment_pipeline():
    posts = extract_posts()
    scored = score_sentiment(posts)
    blob_path = upload_to_blob(scored)
    load_to_snowflake(scored, blob_path)

reddit_sentiment_pipeline()
```

**Important XCom note**: When `scored` is passed to both `upload_to_blob` and `load_to_snowflake`, Airflow will correctly handle this as two downstream dependencies from the same upstream task. This is valid in the TaskFlow API.

Add a module-level docstring explaining what this DAG does, schedule, and dependencies.

---

### STEP 27 — Write dbt/models/staging/stg_reddit_sentiment.sql

Write a dbt staging model `stg_reddit_sentiment.sql` that:

- Sources from `IPL_DB.RAW.REDDIT_IPL_POSTS`
- Define as a dbt source in `sources.yml` with name `raw`, table `REDDIT_IPL_POSTS`
- Selects and renames:
  - `post_id` → `post_id`
  - `post_title` → `title`
  - `post_author` → `author`
  - `post_body` → `body`
  - `score` → `reddit_score`
  - `num_comments` → `num_comments`
  - `created_utc` → `post_created_at`
  - `fetched_at` → `fetched_at`
  - `pipeline_loaded_at` → `loaded_at`
  - `sentiment_compound` → `sentiment_score`
  - `sentiment_label` → `sentiment_label`
  - `sentiment_positive` → `sentiment_pos`
  - `sentiment_negative` → `sentiment_neg`
  - `sentiment_neutral` → `sentiment_neu`
- Add derived column: `post_date` = `CAST(post_created_at AS DATE)`
- Materialised as **view** (lightweight, always fresh, correct for staging)
- Add model description and column descriptions in `schema.yml`

---

### STEP 28 — Write dbt/models/marts/mart_ipl_sentiment.sql

Write a dbt mart model `mart_ipl_sentiment.sql` that aggregates sentiment by date:

- Sources from `{{ ref('stg_reddit_sentiment') }}`
- Groups by `post_date`
- Aggregates:
  - `total_posts` = COUNT(*)
  - `avg_sentiment_score` = AVG(sentiment_score) rounded to 4 decimal places
  - `positive_posts` = COUNTIF(sentiment_label = 'POSITIVE') using `SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END)`
  - `negative_posts` = same pattern for 'NEGATIVE'
  - `neutral_posts` = same pattern for 'NEUTRAL'
  - `positive_pct` = positive_posts / total_posts * 100 rounded to 2 decimal places
  - `negative_pct` = negative_posts / total_posts * 100 rounded to 2 decimal places
  - `max_sentiment_score` = MAX(sentiment_score)
  - `min_sentiment_score` = MIN(sentiment_score)
- Orders by `post_date DESC`
- Materialised as **table** (consumed by Tableau — tables are faster than views for BI tools)
- Add description in `schema.yml`: "Daily IPL sentiment summary from Reddit r/Cricket. Refreshed every 10 minutes by the Airflow micro-batch pipeline."

---

### STEP 29 — Update requirements.txt

Add these packages to `requirements.txt` if not already present:
```
vaderSentiment==3.3.2
requests==2.31.0
```

Do not remove or change existing entries. Only append if missing.

---

### STEP 30 — Update .env.example

Add these lines to `.env.example` under a new comment block if not already present:
```
# Reddit Micro-Batch Sentiment Pipeline
REDDIT_SUBREDDIT=Cricket
REDDIT_SEARCH_QUERY=IPL
REDDIT_MAX_POSTS=25
AZURE_BLOB_REDDIT_PREFIX=reddit/raw
```

Do not remove or change existing entries. Only append if missing.

