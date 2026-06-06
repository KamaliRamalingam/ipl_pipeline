{{ config(materialized='table') }}

WITH sentiment AS (
    SELECT * FROM {{ ref('stg_reddit_sentiment') }}
)

SELECT
    post_date,
    COUNT(*)                                                                AS total_posts,
    ROUND(AVG(sentiment_score), 4)                                          AS avg_sentiment_score,
    SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END)          AS positive_posts,
    SUM(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END)          AS negative_posts,
    SUM(CASE WHEN sentiment_label = 'NEUTRAL'  THEN 1 ELSE 0 END)          AS neutral_posts,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'POSITIVE' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                                       AS positive_pct,
    ROUND(
        SUM(CASE WHEN sentiment_label = 'NEGATIVE' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                                       AS negative_pct,
    MAX(sentiment_score)                                                    AS max_sentiment_score,
    MIN(sentiment_score)                                                    AS min_sentiment_score
FROM sentiment
GROUP BY post_date
ORDER BY post_date DESC
