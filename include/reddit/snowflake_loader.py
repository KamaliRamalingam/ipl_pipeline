import json
import os
from datetime import datetime, timezone

import snowflake.connector

from ingestion.utils import get_logger

logger = get_logger(__name__)

_INSERT_SQL = """
INSERT INTO IPL_DB.RAW.REDDIT_IPL_POSTS (
    PIPELINE_LOAD_ID, PIPELINE_LOADED_AT, PIPELINE_BATCH_FILE,
    POST_ID, POST_TITLE, POST_AUTHOR, SUBREDDIT, POST_URL, POST_BODY,
    SCORE, UPVOTE_RATIO, NUM_COMMENTS,
    CREATED_UTC, FETCHED_AT,
    SENTIMENT_COMPOUND, SENTIMENT_POSITIVE, SENTIMENT_NEGATIVE, SENTIMENT_NEUTRAL, SENTIMENT_LABEL,
    RAW_JSON
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def get_snowflake_connection():
    """Return an open Snowflake connection pointed at the RAW schema.

    Reads credentials from environment variables: SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE.

    Returns:
        A snowflake.connector.SnowflakeConnection instance.
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )


def load_reddit_posts(posts: list[dict], blob_path: str, load_id: str) -> int:
    """Insert a batch of scored Reddit posts into IPL_DB.RAW.REDDIT_IPL_POSTS.

    Each row stores the raw post metadata, VADER sentiment scores, the Azure
    Blob path of the source file, and the full post dict as a Snowflake
    VARIANT column (RAW_JSON) for schema-on-read flexibility.

    The ``load_id`` ties every row in this batch to a single DAG run so that
    individual loads are fully traceable.

    Args:
        posts: List of scored post dicts produced by ``score_all_posts()``.
            Each dict must contain all fields from ``fetch_reddit_posts()``
            plus the five sentiment keys added by ``score_post()``.
        blob_path: Azure Blob path returned by ``upload_reddit_batch()``.
            Stored in PIPELINE_BATCH_FILE for end-to-end traceability.
        load_id: UUID string generated once per DAG run and shared across
            all rows in this batch.

    Returns:
        Number of rows inserted.

    Raises:
        Exception: Re-raises any error after logging, so the Airflow task
            is marked as failed and retried according to DAG retry policy.
    """
    pipeline_loaded_at = datetime.now(timezone.utc)

    rows = [
        (
            load_id,
            pipeline_loaded_at,
            blob_path,
            post.get("post_id"),
            post.get("post_title"),
            post.get("post_author"),
            post.get("subreddit"),
            post.get("post_url"),
            post.get("post_body"),
            post.get("score"),
            post.get("upvote_ratio"),
            post.get("num_comments"),
            post.get("created_utc"),
            post.get("fetched_at"),
            post.get("sentiment_compound"),
            post.get("sentiment_positive"),
            post.get("sentiment_negative"),
            post.get("sentiment_neutral"),
            post.get("sentiment_label"),
            json.dumps(post, default=str),
        )
        for post in posts
    ]

    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        try:
            cursor.executemany(_INSERT_SQL, rows)
            conn.commit()
        finally:
            cursor.close()
    except Exception:
        logger.error(
            "Failed to load Reddit posts to Snowflake (batch: %s)", load_id
        )
        raise
    finally:
        conn.close()

    n = len(rows)
    logger.info(
        "Loaded %d Reddit posts to Snowflake RAW.REDDIT_IPL_POSTS (batch: %s)",
        n,
        load_id,
    )
    return n
