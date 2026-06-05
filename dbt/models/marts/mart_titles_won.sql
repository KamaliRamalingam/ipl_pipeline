{{ config(
    materialized='table',
    schema='MARTS'
) }}

SELECT
    MATCH_WINNER                                        AS team_name,
    COUNT(*)                                            AS titles_won,
    MIN(SEASON)                                         AS first_title_season,
    MAX(SEASON)                                         AS last_title_season,
    LISTAGG(SEASON, ', ')
        WITHIN GROUP (ORDER BY SEASON)                  AS title_seasons
FROM {{ ref('fct_match_results') }}
WHERE EVENT_STAGE = 'Final'
  AND MATCH_WINNER IS NOT NULL
GROUP BY MATCH_WINNER
ORDER BY titles_won DESC