{{ config(materialized='table') }}

WITH teams AS (
    SELECT team1 AS team_name FROM {{ ref('stg_matches') }}
    UNION
    SELECT team2 AS team_name FROM {{ ref('stg_matches') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id,
    team_name
FROM teams
WHERE team_name IS NOT NULL
