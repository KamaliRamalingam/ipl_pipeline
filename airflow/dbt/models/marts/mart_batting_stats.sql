{{ config(materialized='table') }}

WITH innings_scores AS (
    SELECT
        d.batter,
        m.season,
        d.match_id,
        d.inning_number,
        SUM(d.runs_batter)                          AS innings_runs,
        COUNT(*)                                     AS balls_in_innings,
        SUM(CASE WHEN d.is_four THEN 1 ELSE 0 END)  AS fours_in_innings,
        SUM(CASE WHEN d.is_six  THEN 1 ELSE 0 END)  AS sixes_in_innings
    FROM {{ ref('fct_deliveries') }} d
    JOIN {{ ref('fct_match_results') }} m ON d.match_id = m.match_id
    GROUP BY d.batter, m.season, d.match_id, d.inning_number
)

SELECT
    batter                                                                      AS player_name,
    season,
    COUNT(*)                                                                    AS innings_played,
    SUM(innings_runs)                                                           AS total_runs,
    SUM(balls_in_innings)                                                       AS balls_faced,
    ROUND(SUM(innings_runs) / NULLIF(SUM(balls_in_innings), 0) * 100, 2)        AS strike_rate,
    ROUND(SUM(innings_runs) / NULLIF(COUNT(*), 0), 2)                           AS average,
    SUM(fours_in_innings)                                                       AS fours,
    SUM(sixes_in_innings)                                                       AS sixes,
    SUM(fours_in_innings + sixes_in_innings)                                    AS boundary_count,
    MAX(innings_runs)                                                           AS highest_score
FROM innings_scores
GROUP BY batter, season
