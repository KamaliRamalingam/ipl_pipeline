-- =============================================================
-- IPL Analytics Pipeline — Snowflake Setup
-- Author: Kamali Ramalingam
-- Run once as ACCOUNTADMIN to initialise the full environment
-- =============================================================
USE ROLE ACCOUNTADMIN;

-- =============================================================
-- BLOCK 1 — Virtual Warehouse
-- X-SMALL is sufficient for this dataset (~295k rows)
-- AUTO_SUSPEND saves credits when idle
-- AUTO_RESUME ensures queries never wait for manual start
-- =============================================================
CREATE WAREHOUSE IF NOT EXISTS IPL_WH
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- =============================================================
-- BLOCK 2 — Database and Schemas (Medallion Architecture)
-- RAW      : exact data from source, no transformations
-- STAGING  : cleaned and typed by dbt
-- MARTS    : aggregated, business-ready tables for Tableau
-- =============================================================
CREATE DATABASE IF NOT EXISTS IPL_DB;
CREATE SCHEMA IF NOT EXISTS IPL_DB.RAW;
CREATE SCHEMA IF NOT EXISTS IPL_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS IPL_DB.MARTS;

-- =============================================================
-- BLOCK 3 — Roles and Privileges (RBAC)
-- Each role gets minimum privilege needed for its function
-- INGESTION_ROLE : write to RAW only (load_to_snowflake.py)
-- DBT_ROLE       : read RAW, write STAGING and MARTS
-- REPORTING_ROLE : read-only on MARTS (Tableau)
-- =============================================================

-- ROLES
CREATE ROLE IF NOT EXISTS INGESTION_ROLE;   -- used by load_to_snowflake.py
CREATE ROLE IF NOT EXISTS DBT_ROLE;         -- used by dbt transformations
CREATE ROLE IF NOT EXISTS REPORTING_ROLE;   -- used by Tableau / analysts

-- WAREHOUSE ACCESS
GRANT USAGE ON WAREHOUSE IPL_WH TO ROLE INGESTION_ROLE;
GRANT USAGE ON WAREHOUSE IPL_WH TO ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE IPL_WH TO ROLE REPORTING_ROLE;

-- DATABASE ACCESS
GRANT USAGE ON DATABASE IPL_DB TO ROLE INGESTION_ROLE;
GRANT USAGE ON DATABASE IPL_DB TO ROLE DBT_ROLE;
GRANT USAGE ON DATABASE IPL_DB TO ROLE REPORTING_ROLE;

-- INGESTION_ROLE: write access to RAW only
GRANT USAGE ON SCHEMA IPL_DB.RAW TO ROLE INGESTION_ROLE;
GRANT CREATE TABLE ON SCHEMA IPL_DB.RAW TO ROLE INGESTION_ROLE;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA IPL_DB.RAW TO ROLE INGESTION_ROLE;
GRANT INSERT, UPDATE ON FUTURE TABLES IN SCHEMA IPL_DB.RAW TO ROLE INGESTION_ROLE;

-- DBT_ROLE: read RAW, write STAGING and MARTS
GRANT USAGE ON SCHEMA IPL_DB.RAW TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA IPL_DB.RAW TO ROLE DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA IPL_DB.RAW TO ROLE DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA IPL_DB.STAGING TO ROLE DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA IPL_DB.MARTS TO ROLE DBT_ROLE;

-- REPORTING_ROLE: read-only on MARTS
GRANT USAGE ON SCHEMA IPL_DB.MARTS TO ROLE REPORTING_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA IPL_DB.MARTS TO ROLE REPORTING_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA IPL_DB.MARTS TO ROLE REPORTING_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA IPL_DB.MARTS TO ROLE REPORTING_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IPL_DB.MARTS TO ROLE REPORTING_ROLE;

-- ASSIGN ROLES TO USER
GRANT ROLE INGESTION_ROLE TO USER VRPKAMALI;
GRANT ROLE DBT_ROLE TO USER VRPKAMALI;
GRANT ROLE REPORTING_ROLE TO USER VRPKAMALI;

-- =============================================================
-- BLOCK 4 — RAW Tables
-- Column names match exactly what parse_cricsheet.py produces
-- loaded_at is an audit column — tracks when each row was inserted
-- NULL is allowed on optional fields (extras, wickets, city)
-- =============================================================
USE WAREHOUSE IPL_WH;
USE SCHEMA IPL_DB.RAW;

-- Raw matches table: one row per match, exact fields from cricsheet JSON
CREATE TABLE IF NOT EXISTS RAW_MATCHES (
    match_id             VARCHAR(50),
    match_date           DATE,
    season               VARCHAR(10),
    team1                VARCHAR(100),
    team2                VARCHAR(100),
    venue                VARCHAR(200),
    city                 VARCHAR(100),
    toss_winner          VARCHAR(100),
    toss_decision        VARCHAR(10),
    match_winner         VARCHAR(100),
    win_by_runs          INTEGER,
    win_by_wickets       INTEGER,
    player_of_match      VARCHAR(100),
    event_name           VARCHAR(100),
    event_stage          VARCHAR(100),
    event_match_number   INTEGER,
    loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw deliveries table: one row per ball, exact fields from cricsheet JSON
CREATE TABLE IF NOT EXISTS RAW_DELIVERIES (
    match_id             VARCHAR(50),
    inning_number        INTEGER,
    batting_team         VARCHAR(100),
    over_number          INTEGER,
    ball_number          INTEGER,
    batter               VARCHAR(100),
    bowler               VARCHAR(100),
    non_striker          VARCHAR(100),
    runs_batter          INTEGER,
    runs_extras          INTEGER,
    runs_total           INTEGER,
    extras_type          VARCHAR(50),
    is_wicket            BOOLEAN,
    wicket_kind          VARCHAR(50),
    player_out           VARCHAR(100),
    fielder              VARCHAR(100),
    loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);




-- =============================================================
-- BLOCK 5 — Reddit Sentiment Raw Table
-- Populated by Airflow reddit_sentiment_pipeline DAG (every 10 min)
-- VARIANT column RAW_JSON stores the full post for schema-on-read
-- =============================================================
CREATE TABLE IF NOT EXISTS IPL_DB.RAW.REDDIT_IPL_POSTS (
    PIPELINE_LOAD_ID        VARCHAR(64)     NOT NULL,
    PIPELINE_LOADED_AT      TIMESTAMP_NTZ   NOT NULL,
    PIPELINE_BATCH_FILE     VARCHAR(512),
    POST_ID                 VARCHAR(32)     NOT NULL,
    POST_TITLE              VARCHAR(4000),
    POST_AUTHOR             VARCHAR(256),
    SUBREDDIT               VARCHAR(128),
    POST_URL                VARCHAR(2048),
    POST_BODY               VARCHAR(40000),
    SCORE                   NUMBER,
    UPVOTE_RATIO            FLOAT,
    NUM_COMMENTS            NUMBER,
    CREATED_UTC             TIMESTAMP_NTZ,
    FETCHED_AT              TIMESTAMP_NTZ   NOT NULL,
    SENTIMENT_COMPOUND      FLOAT,
    SENTIMENT_POSITIVE      FLOAT,
    SENTIMENT_NEGATIVE      FLOAT,
    SENTIMENT_NEUTRAL       FLOAT,
    SENTIMENT_LABEL         VARCHAR(16),
    RAW_JSON                VARIANT
);

-- Grant access to existing roles
GRANT SELECT ON TABLE IPL_DB.RAW.REDDIT_IPL_POSTS TO ROLE DBT_ROLE;
GRANT INSERT ON TABLE IPL_DB.RAW.REDDIT_IPL_POSTS TO ROLE INGESTION_ROLE;
