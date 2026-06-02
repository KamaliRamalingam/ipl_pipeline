{{ config(materialized='table') }}

WITH venues AS (
    SELECT venue, MAX(city) AS city
    FROM {{ ref('stg_matches') }}
    WHERE venue IS NOT NULL
    GROUP BY venue
)

SELECT
    ROW_NUMBER() OVER (ORDER BY venue) AS venue_id,
    venue,
    city
FROM venues
