{{ config(materialized='view') }}

SELECT
    match_id,
    match_date::DATE                                        AS match_date,
    CASE season
        WHEN '2007/08' THEN '2008'
        WHEN '2009/10' THEN '2010'
        ELSE season
    END                                                     AS season,
    team1,
    team2,
    venue,
    city,
    toss_winner,
    toss_decision,
    match_winner,
    win_by_runs::INTEGER                                    AS win_by_runs,
    win_by_wickets::INTEGER                                 AS win_by_wickets,
    player_of_match,
    event_name,
    event_stage,
    event_match_number,
    CASE
        WHEN win_by_runs    IS NOT NULL THEN 'runs'
        WHEN win_by_wickets IS NOT NULL THEN 'wickets'
        ELSE 'tie/no result'
    END                                                     AS result_type,
    loaded_at
FROM IPL_DB.RAW.RAW_MATCHES
