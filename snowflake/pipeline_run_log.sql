-- Pipeline audit log table
-- Tracks last run time for each Airflow DAG
-- Updated by log_pipeline_run task after each successful run

CREATE OR REPLACE TABLE IPL_DB.MARTS.PIPELINE_RUN_LOG (
    PIPELINE_NAME  VARCHAR,
    RUN_TYPE       VARCHAR,
    LAST_RUN_AT    TIMESTAMP_TZ,
    STATUS         VARCHAR
);
