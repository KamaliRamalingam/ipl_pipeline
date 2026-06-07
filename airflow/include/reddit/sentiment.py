from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ingestion.utils import get_logger
from include.reddit.extractor import get_text_for_sentiment

logger = get_logger(__name__)


def get_sentiment_analyser() -> SentimentIntensityAnalyzer:
    """Initialise and return a VADER SentimentIntensityAnalyzer instance.

    VADER is a rule-based sentiment analyser tuned for social media text.
    Instantiation loads the lexicon from disk, so callers should create one
    instance and reuse it across all posts in a batch.

    Returns:
        A ready-to-use SentimentIntensityAnalyzer.
    """
    analyser = SentimentIntensityAnalyzer()
    logger.info("VADER sentiment analyser initialised")
    return analyser


def score_post(post: dict, analyser: SentimentIntensityAnalyzer) -> dict:
    """Add VADER sentiment scores to a single post dict.

    Scoring is performed on the concatenation of title and body produced by
    ``get_text_for_sentiment()``. If that text is empty all score fields are
    set to None and the label defaults to "NEUTRAL".

    Compound thresholds:
        >= 0.05  → "POSITIVE"
        <= -0.05 → "NEGATIVE"
        else     → "NEUTRAL"

    Args:
        post: A post dict as returned by ``fetch_reddit_posts()``.
        analyser: A SentimentIntensityAnalyzer instance.

    Returns:
        The original post dict with five new keys added:
        ``sentiment_compound``, ``sentiment_positive``, ``sentiment_negative``,
        ``sentiment_neutral``, ``sentiment_label``.
    """
    text = get_text_for_sentiment(post)

    if not text:
        post["sentiment_compound"] = None
        post["sentiment_positive"] = None
        post["sentiment_negative"] = None
        post["sentiment_neutral"] = None
        post["sentiment_label"] = "NEUTRAL"
        return post

    scores = analyser.polarity_scores(text)
    compound = scores["compound"]

    if compound >= 0.05:
        label = "POSITIVE"
    elif compound <= -0.05:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"

    post["sentiment_compound"] = compound
    post["sentiment_positive"] = scores["pos"]
    post["sentiment_negative"] = scores["neg"]
    post["sentiment_neutral"] = scores["neu"]
    post["sentiment_label"] = label
    return post


def score_all_posts(
    posts: list[dict], analyser: SentimentIntensityAnalyzer
) -> list[dict]:
    """Score sentiment for every post in a batch.

    Args:
        posts: List of post dicts from ``fetch_reddit_posts()``.
        analyser: A SentimentIntensityAnalyzer instance.

    Returns:
        List of post dicts, each augmented with sentiment score fields.
    """
    scored = [score_post(post, analyser) for post in posts]
    logger.info("Scored sentiment for %d posts", len(scored))
    return scored
