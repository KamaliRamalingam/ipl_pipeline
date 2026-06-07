from datetime import datetime

import requests

from ingestion.utils import get_logger

logger = get_logger(__name__)

_HEADERS = {"User-Agent": "ipl-sentiment-pipeline/1.0"}


def fetch_reddit_posts(
    subreddit: str,
    query: str,
    limit: int,
    fetched_at: datetime,
) -> list[dict]:
    """Fetch recent posts from a public Reddit subreddit using the JSON API.

    No API key is required. Uses the Arctic Shift API which is less
    aggressively rate-limited than the Reddit public JSON API.

    Args:
        subreddit: Subreddit name to search, e.g. "Cricket".
        query: Search term, e.g. "IPL".
        limit: Maximum number of posts to return.
        fetched_at: Timestamp of when this fetch was initiated. Passed in
            rather than generated here so that tests can control the value.

    Returns:
        List of post dicts. Each dict contains post metadata plus
        ``fetched_at``. Returns an empty list if no posts are found or the
        response structure is unexpected.

    Raises:
        requests.HTTPError: If the API returns a non-200 status code.
    """
    url = f"https://arctic-shift.photon-reddit.com/api/posts/search"
    params = {
        "subreddit": subreddit,
        "limit": limit,
        "after": "2026-01-01",
    }
    headers = {"User-Agent": "ipl-sentiment-pipeline/1.0"}

    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()

    posts_data = response.json().get("data", [])
    if not posts_data:
        return []

    posts = []
    for post in posts_data:
        posts.append(
            {
                "post_id": post.get("id"),
                "post_title": post.get("title"),
                "post_author": post.get("author"),
                "subreddit": post.get("subreddit"),
                "post_url": post.get("url"),
                "post_body": post.get("selftext", ""),
                "score": post.get("score"),
                "upvote_ratio": post.get("upvote_ratio"),
                "num_comments": post.get("num_comments"),
                "created_utc": datetime.utcfromtimestamp(post.get("created_utc", 0)),
                "fetched_at": fetched_at,
            }
        )

    logger.info("Fetched %d posts from r/%s (query=%r)", len(posts), subreddit, query)
    return posts


def get_text_for_sentiment(post: dict) -> str:
    """Combine post title and body into a single string for sentiment scoring.

    Args:
        post: A post dict as returned by ``fetch_reddit_posts()``.

    Returns:
        Stripped concatenation of title and body. Returns empty string if
        both fields are absent or empty.
    """
    title = (post.get("post_title") or "").strip()
    body = (post.get("post_body") or "").strip()
    text = f"{title} {body}".strip()
    return text
