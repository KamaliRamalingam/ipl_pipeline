{{ config(materialized='table') }}

WITH bowling_per_match AS (
    SELECT
        d.bowler,
        m.season,
        d.match_id,
        COUNT(*)                                            AS balls_bowled,
        SUM(d.runs_total)                                   AS runs_conceded,
        SUM(CASE WHEN d.is_wicket THEN 1 ELSE 0 END)        AS wickets
    FROM {{ ref('fct_deliveries') }} d
    JOIN {{ ref('fct_match_results') }} m ON d.match_id = m.match_id
    GROUP BY d.bowler, m.season, d.match_id
),
best_figures AS (
    SELECT DISTINCT
        bowler,
        season,
        FIRST_VALUE(CONCAT(wickets::VARCHAR, '/', runs_conceded::VARCHAR)) OVER (
            PARTITION BY bowler, season
            ORDER BY wickets DESC, runs_conceded ASC
        ) AS best_bowling_figures
    FROM bowling_per_match
)

SELECT
    bm.bowler                                                                           AS player_name,
    bm.season,
    SUM(bm.balls_bowled)                                                                AS balls_bowled,
    ROUND(SUM(bm.balls_bowled) / 6.0, 1)                                                AS overs_bowled,
    SUM(bm.runs_conceded)                                                               AS runs_conceded,
    SUM(bm.wickets)                                                                     AS total_wickets,
    ROUND(SUM(bm.runs_conceded) / NULLIF(ROUND(SUM(bm.balls_bowled) / 6.0, 1), 0), 2)  AS economy_rate,
    ROUND(SUM(bm.runs_conceded) / NULLIF(SUM(bm.wickets), 0), 2)                        AS bowling_average,
    ROUND(SUM(bm.balls_bowled)  / NULLIF(SUM(bm.wickets), 0), 2)                        AS bowling_strike_rate,
    bf.best_bowling_figures
FROM bowling_per_match bm
JOIN best_figures bf ON bm.bowler = bf.bowler AND bm.season = bf.season
GROUP BY bm.bowler, bm.season, bf.best_bowling_figures
