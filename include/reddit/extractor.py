from datetime import datetime

import requests

from ingestion.utils import get_logger

logger = get_logger(__name__)

_HEADERS = {"User-Agent": "ipl-sentiment-pipeline/1.0 (educational project)"}


def fetch_reddit_posts(
    subreddit: str,
    query: str,
    limit: int,
    fetched_at: datetime,
) -> list[dict]:
    """Fetch recent posts from a public Reddit subreddit using the JSON API.

    No API key is required. Reddit's public search endpoint is used with a
    custom User-Agent header to avoid 429 rate-limit responses.

    Args:
        subreddit: Subreddit name to search, e.g. "Cricket".
        query: Search term, e.g. "IPL".
        limit: Maximum number of posts to return (Reddit caps at 100).
        fetched_at: Timestamp of when this fetch was initiated. Passed in
            rather than generated here so that tests can control the value.

    Returns:
        List of post dicts. Each dict contains post metadata plus
        ``fetched_at``. Returns an empty list if no posts are found or the
        response structure is unexpected.

    Raises:
        requests.HTTPError: If Reddit returns a non-200 status code.
    """
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    params = {"q": query, "sort": "new", "limit": limit, "t": "hour"}

    response = requests.get(url, headers=_HEADERS, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()
    children = data.get("data", {}).get("children", [])

    posts = []
    for post in children:
        pd = post.get("data", {})
        posts.append(
            {
                "post_id": pd.get("id"),
                "post_title": pd.get("title"),
                "post_author": pd.get("author"),
                "subreddit": pd.get("subreddit"),
                "post_url": pd.get("url"),
                "post_body": pd.get("selftext", ""),
                "score": pd.get("score"),
                "upvote_ratio": pd.get("upvote_ratio"),
                "num_comments": pd.get("num_comments"),
                "created_utc": datetime.utcfromtimestamp(pd["created_utc"])
                if pd.get("created_utc") is not None
                else None,
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
