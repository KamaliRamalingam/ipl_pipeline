"""Reddit IPL Sentiment Micro-Batch DAG.

Runs every 15 minutes to fetch recent posts from Reddit r/Cricket, score
them with VADER sentiment analysis, archive the raw JSON to Azure Blob
Storage, and load the scored rows into Snowflake RAW.REDDIT_IPL_POSTS.

Schedule: */15 * * * *  (every 15 minutes)
max_active_runs=1 prevents overlapping runs if a batch takes longer than
15 minutes (e.g. slow Snowflake connection).

Dependencies (task order):
    extract_posts → score_sentiment → upload_to_blob → load_to_snowflake
                                   ↘────────────────────────────────────↗
    (scored posts flow to both upload_to_blob and load_to_snowflake)
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import requests as http_requests

from airflow.sdk import dag, task

from ingestion.utils import get_logger

logger = get_logger(__name__)

default_args = {
    "owner": "kamali",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


@dag(
    dag_id="reddit_sentiment_pipeline",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "sentiment", "ipl", "micro-batch"],
    default_args=default_args,
)
def reddit_sentiment_pipeline():
    """Orchestrate the Reddit IPL sentiment micro-batch pipeline."""

    @task()
    def extract_posts() -> list[dict]:
        """Fetch recent IPL-related posts from Reddit r/Cricket.

        Reads REDDIT_SUBREDDIT, REDDIT_SEARCH_QUERY, and REDDIT_MAX_POSTS
        from environment variables. Datetime fields are converted to ISO
        strings before returning so Airflow XCom can serialise the dicts
        as JSON.

        Returns:
            List of post dicts, or empty list if no posts were found.
        """
        from include.reddit.extractor import fetch_reddit_posts

        subreddit = os.environ.get("REDDIT_SUBREDDIT", "Cricket")
        query = os.environ.get("REDDIT_SEARCH_QUERY", "IPL")
        limit = int(os.environ.get("REDDIT_MAX_POSTS", 25))
        fetched_at = datetime.now(timezone.utc)

        posts = fetch_reddit_posts(subreddit, query, limit, fetched_at)

        if not posts:
            logger.warning("No posts fetched — Reddit API may be rate limiting")
            return []

        for post in posts:
            for key, value in post.items():
                if isinstance(value, datetime):
                    post[key] = value.isoformat()

        return posts

    @task()
    def score_sentiment(posts: list[dict]) -> list[dict]:
        """Score VADER sentiment for each post.

        Args:
            posts: List of post dicts from ``extract_posts()``.

        Returns:
            List of post dicts augmented with sentiment score fields,
            or empty list if input is empty.
        """
        from include.reddit.sentiment import get_sentiment_analyser, score_all_posts

        if not posts:
            logger.info("No posts to score, skipping")
            return []

        analyser = get_sentiment_analyser()
        return score_all_posts(posts, analyser)

    @task()
    def upload_to_blob(scored_posts: list[dict]) -> str:
        """Upload the scored post batch as JSON to Azure Blob Storage.

        Reconstructs ``fetched_at`` from the first post's ISO string so the
        blob path matches the original fetch timestamp.

        Args:
            scored_posts: Scored post dicts from ``score_sentiment()``.

        Returns:
            Blob path string, or empty string if there are no posts.
        """
        from include.reddit.blob_uploader import upload_reddit_batch

        if not scored_posts:
            logger.info("No posts to upload, skipping")
            return ""

        container_name = os.environ["AZURE_CONTAINER_NAME"]
        blob_prefix = os.environ.get("AZURE_BLOB_REDDIT_PREFIX", "reddit/raw")
        fetched_at = datetime.fromisoformat(scored_posts[0]["fetched_at"])

        return upload_reddit_batch(scored_posts, fetched_at, container_name, blob_prefix)

    @task()
    def load_to_snowflake(scored_posts: list[dict], blob_path: str) -> None:
        """Load scored posts into Snowflake RAW.REDDIT_IPL_POSTS.

        Generates a UUID load_id for this batch so every inserted row can be
        traced back to a single DAG run.

        Args:
            scored_posts: Scored post dicts from ``score_sentiment()``.
            blob_path: Azure Blob path returned by ``upload_to_blob()``.
        """
        from include.reddit.snowflake_loader import load_reddit_posts

        if not scored_posts:
            logger.info("No posts to load, skipping")
            return

        load_id = str(uuid.uuid4())
        load_reddit_posts(scored_posts, blob_path, load_id)
        logger.info(
            "Micro-batch complete. load_id=%s, posts=%d, blob=%s",
            load_id,
            len(scored_posts),
            blob_path,
        )

    @task()
    def trigger_dbt_job() -> None:
        """Trigger dbt Cloud job to refresh stg_reddit_sentiment and mart_ipl_sentiment."""
        account_id = os.environ["DBT_ACCOUNT_ID"]
        job_id = os.environ["DBT_REDDIT_JOB_ID"]
        api_token = os.environ["DBT_API_TOKEN"]
        base_url = os.environ.get("DBT_BASE_URL", "https://cloud.getdbt.com")

        url = f"{base_url}/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
        headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json",
        }
        payload = {"cause": "Triggered by Airflow reddit_sentiment_pipeline"}

        response = http_requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        run_id = response.json()["data"]["id"]
        logger.info("dbt Cloud job triggered successfully. Run ID: %s", run_id)

    @task()
    def log_pipeline_run() -> None:
        """Merge a SUCCESS row into IPL_DB.MARTS.PIPELINE_RUN_LOG for this run."""
        from ingestion.load_to_snowflake import get_snowflake_connection
        from datetime import datetime, timezone

        run_at = datetime.now(timezone.utc)

        conn = get_snowflake_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    MERGE INTO IPL_DB.MARTS.PIPELINE_RUN_LOG AS target
                    USING (
                        SELECT 'reddit_sentiment_pipeline' AS PIPELINE_NAME,
                            'Micro Batch'               AS RUN_TYPE
                    ) AS source
                    ON  target.PIPELINE_NAME = source.PIPELINE_NAME
                    AND target.RUN_TYPE      = source.RUN_TYPE
                    WHEN MATCHED THEN UPDATE SET
                        LAST_RUN_AT = %(run_at)s,
                        STATUS      = 'SUCCESS'
                    WHEN NOT MATCHED THEN INSERT
                        (PIPELINE_NAME, RUN_TYPE, LAST_RUN_AT, STATUS)
                    VALUES
                        ('reddit_sentiment_pipeline', 'Micro Batch', %(run_at)s, 'SUCCESS')
                """, {"run_at": run_at})
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()
        logger.info("Pipeline run logged to IPL_DB.MARTS.PIPELINE_RUN_LOG")

    posts = extract_posts()
    scored = score_sentiment(posts)
    blob_path = upload_to_blob(scored)
    load_to_snowflake(scored, blob_path) >> trigger_dbt_job() >> log_pipeline_run()


reddit_sentiment_pipeline()
