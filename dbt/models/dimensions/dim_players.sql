{{ config(materialized='table') }}

WITH players AS (
    SELECT batter AS player_name FROM {{ ref('stg_deliveries') }}
    UNION
    SELECT bowler AS player_name FROM {{ ref('stg_deliveries') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY player_name) AS player_id,
    player_name
FROM players
WHERE player_name IS NOT NULL
