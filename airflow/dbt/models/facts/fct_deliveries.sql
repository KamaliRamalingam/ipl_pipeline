{{ config(
    materialized='incremental',
    unique_key='match_id'
) }}

WITH deliveries AS (
    SELECT * FROM {{ ref('stg_deliveries') }}
),
players AS (
    SELECT * FROM {{ ref('dim_players') }}
),
matches AS (
    SELECT * FROM {{ ref('fct_match_results') }}
)

SELECT
    d.match_id,
    d.inning_number,
    d.batting_team,
    d.over_number,
    d.ball_number,
    d.batter,
    d.bowler,
    d.non_striker,
    d.runs_batter,
    d.runs_extras,
    d.runs_total,
    d.extras_type,
    d.is_wicket,
    d.wicket_kind,
    d.player_out,
    d.fielder,
    d.is_boundary,
    d.is_six,
    d.is_four,
    pb.player_id  AS batter_id,
    pw.player_id  AS bowler_id,
    m.season,
    m.match_date,
    m.venue,
    m.result_type
FROM deliveries d
LEFT JOIN players      pb ON d.batter   = pb.player_name
LEFT JOIN players      pw ON d.bowler   = pw.player_name
LEFT JOIN matches      m  ON d.match_id = m.match_id
{% if is_incremental() %}
WHERE d.match_id NOT IN (SELECT DISTINCT match_id FROM {{ this }})
{% endif %}
