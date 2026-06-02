{{ config(materialized='view') }}

SELECT
    match_id,
    inning_number::INTEGER                          AS inning_number,
    batting_team,
    over_number::INTEGER                            AS over_number,
    ball_number::INTEGER                            AS ball_number,
    batter,
    bowler,
    non_striker,
    runs_batter::INTEGER                            AS runs_batter,
    runs_extras::INTEGER                            AS runs_extras,
    runs_total::INTEGER                             AS runs_total,
    extras_type,
    is_wicket::BOOLEAN                              AS is_wicket,
    wicket_kind,
    player_out,
    fielder,
    (runs_batter >= 4)                              AS is_boundary,
    (runs_batter = 6)                               AS is_six,
    (runs_batter = 4)                               AS is_four,
    loaded_at
FROM IPL_DB.RAW.RAW_DELIVERIES
