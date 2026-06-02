{{ config(materialized='table') }}

WITH team_matches AS (
    SELECT season, team1 AS team_name, match_id, match_winner, toss_winner
    FROM {{ ref('fct_match_results') }}
    UNION ALL
    SELECT season, team2 AS team_name, match_id, match_winner, toss_winner
    FROM {{ ref('fct_match_results') }}
),
team_runs AS (
    SELECT
        m.season,
        d.batting_team      AS team_name,
        d.match_id,
        SUM(d.runs_total)   AS match_runs
    FROM {{ ref('fct_deliveries') }} d
    JOIN {{ ref('fct_match_results') }} m ON d.match_id = m.match_id
    GROUP BY m.season, d.batting_team, d.match_id
)

SELECT
    tm.team_name,
    tm.season,
    COUNT(*)                                                                                AS matches_played,
    SUM(CASE WHEN tm.match_winner = tm.team_name THEN 1 ELSE 0 END)                        AS matches_won,
    ROUND(
        SUM(CASE WHEN tm.match_winner = tm.team_name THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                                                       AS win_rate,
    SUM(CASE WHEN tm.toss_winner = tm.team_name THEN 1 ELSE 0 END)                         AS toss_wins,
    ROUND(
        SUM(CASE WHEN tm.toss_winner = tm.team_name AND tm.match_winner = tm.team_name THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN tm.toss_winner = tm.team_name THEN 1 ELSE 0 END), 0) * 100, 2
    )                                                                                       AS toss_win_to_match_win_rate,
    ROUND(AVG(tr.match_runs), 2)                                                            AS avg_runs_per_match
FROM team_matches tm
LEFT JOIN team_runs tr
    ON  tm.team_name = tr.team_name
    AND tm.match_id  = tr.match_id
    AND tm.season    = tr.season
GROUP BY tm.team_name, tm.season
