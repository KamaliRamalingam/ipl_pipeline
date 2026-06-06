{{ config(materialized='view') }}

SELECT
    post_id,
    post_title                          AS title,
    post_author                         AS author,
    post_body                           AS body,
    score                               AS reddit_score,
    num_comments,
    created_utc                         AS post_created_at,
    fetched_at,
    pipeline_loaded_at                  AS loaded_at,
    sentiment_compound                  AS sentiment_score,
    sentiment_label,
    sentiment_positive                  AS sentiment_pos,
    sentiment_negative                  AS sentiment_neg,
    sentiment_neutral                   AS sentiment_neu,
    CAST(created_utc AS DATE)           AS post_date
FROM {{ source('raw', 'REDDIT_IPL_POSTS') }}
