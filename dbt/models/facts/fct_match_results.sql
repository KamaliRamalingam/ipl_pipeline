{{ config(materialized='table') }}

WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
),
teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),
venues AS (
    SELECT * FROM {{ ref('dim_venues') }}
)

SELECT
    m.match_id,
    m.match_date,
    m.season,
    m.team1,
    m.team2,
    m.venue,
    m.city,
    m.toss_winner,
    m.toss_decision,
    m.match_winner,
    m.win_by_runs,
    m.win_by_wickets,
    m.result_type,
    m.player_of_match,
    m.event_name,
    m.event_stage,
    m.event_match_number,
    t1.team_id  AS team1_id,
    t2.team_id  AS team2_id,
    tw.team_id  AS winner_id,
    v.venue_id
FROM matches m
LEFT JOIN teams  t1 ON m.team1        = t1.team_name
LEFT JOIN teams  t2 ON m.team2        = t2.team_name
LEFT JOIN teams  tw ON m.match_winner = tw.team_name
LEFT JOIN venues v  ON m.venue        = v.venue
