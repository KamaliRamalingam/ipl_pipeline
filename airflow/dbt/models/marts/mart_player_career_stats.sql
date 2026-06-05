{{ config(
    materialized='table',
    schema='MARTS'
) }}

WITH batting AS (
    SELECT
        player_name,
        COUNT(DISTINCT season)                                      AS seasons_played,
        SUM(innings_played)                                         AS total_innings,
        SUM(total_runs)                                             AS career_runs,
        SUM(balls_faced)                                            AS career_balls_faced,
        SUM(fours)                                                  AS career_fours,
        SUM(sixes)                                                  AS career_sixes,
        MAX(highest_score)                                          AS highest_score,
        ROUND(
            SUM(total_runs) * 100.0 / NULLIF(SUM(balls_faced), 0)
        , 2)                                                        AS career_strike_rate,
        ROUND(
            SUM(total_runs) * 1.0 / NULLIF(SUM(innings_played), 0)
        , 2)                                                        AS career_average
    FROM {{ ref('mart_batting_stats') }}
    GROUP BY player_name
),

bowling AS (
    SELECT
        player_name,
        SUM(total_wickets)                                          AS career_wickets,
        SUM(balls_bowled)                                           AS career_balls_bowled,
        ROUND(
            SUM(runs_conceded) / NULLIF(SUM(overs_bowled), 0)
        , 2)                                                        AS career_economy,
        ROUND(
            SUM(runs_conceded) * 1.0 / NULLIF(SUM(total_wickets), 0)
        , 2)                                                        AS career_bowling_average
    FROM {{ ref('mart_bowling_stats') }}
    GROUP BY player_name
)

SELECT
    b.player_name,
    b.seasons_played,
    b.total_innings,
    b.career_runs,
    b.career_balls_faced,
    b.career_strike_rate,
    b.career_average,
    b.career_fours,
    b.career_sixes,
    b.highest_score,
    COALESCE(bo.career_wickets, 0)              AS career_wickets,
    bo.career_economy,
    bo.career_bowling_average
FROM batting b
LEFT JOIN bowling bo
    ON b.player_name = bo.player_name
ORDER BY b.career_runs DESC