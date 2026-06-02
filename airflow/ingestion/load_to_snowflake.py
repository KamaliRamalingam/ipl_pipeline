import snowflake.connector

from ingestion.parse_cricsheet import parse_all_files
from ingestion.utils import get_logger, load_env

logger = get_logger(__name__)


def get_snowflake_connection():
    """Return an open Snowflake connection using environment variables.

    Returns:
        A snowflake.connector.SnowflakeConnection instance.
    """
    env = load_env()
    return snowflake.connector.connect(
        account=env["SNOWFLAKE_ACCOUNT"],
        user=env["SNOWFLAKE_USER"],
        password=env["SNOWFLAKE_PASSWORD"],
        database=env["SNOWFLAKE_DATABASE"],
        schema=env["SNOWFLAKE_SCHEMA"],
        warehouse=env["SNOWFLAKE_WAREHOUSE"],
        role=env["SNOWFLAKE_ROLE"],
    )


def create_raw_tables(conn):
    """Create RAW.RAW_MATCHES and RAW.RAW_DELIVERIES tables if they do not already exist.

    Args:
        conn: An open Snowflake connection.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_MATCHES (
                match_id           VARCHAR,
                match_date         DATE,
                season             VARCHAR,
                team1              VARCHAR,
                team2              VARCHAR,
                venue              VARCHAR,
                city               VARCHAR,
                toss_winner        VARCHAR,
                toss_decision      VARCHAR,
                match_winner       VARCHAR,
                win_by_runs        INTEGER,
                win_by_wickets     INTEGER,
                player_of_match    VARCHAR,
                event_name         VARCHAR,
                event_stage        VARCHAR,
                event_match_number INTEGER,
                loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("RAW.RAW_MATCHES table ready")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.RAW_DELIVERIES (
                match_id       VARCHAR,
                inning_number  INTEGER,
                batting_team   VARCHAR,
                over_number    INTEGER,
                ball_number    INTEGER,
                batter         VARCHAR,
                bowler         VARCHAR,
                non_striker    VARCHAR,
                runs_batter    INTEGER,
                runs_extras    INTEGER,
                runs_total     INTEGER,
                extras_type    VARCHAR,
                is_wicket      BOOLEAN,
                wicket_kind    VARCHAR,
                player_out     VARCHAR,
                fielder        VARCHAR,
                loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("RAW.RAW_DELIVERIES table ready")
    finally:
        cursor.close()


def load_matches(conn, matches):
    """Insert new match records into RAW.RAW_MATCHES, skipping duplicates.

    Checks existing match_ids first and only inserts records not already present.

    Args:
        conn: An open Snowflake connection.
        matches: List of match dicts from parse_match_info().
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT match_id FROM RAW.RAW_MATCHES")
        existing_ids = {row[0] for row in cursor.fetchall()}

        new_matches = [m for m in matches if m["match_id"] not in existing_ids]
        skipped = len(matches) - len(new_matches)

        if new_matches:
            rows = [
                (
                    m["match_id"],
                    m["match_date"],
                    str(m["season"]),
                    m["team1"],
                    m["team2"],
                    m["venue"],
                    m["city"],
                    m["toss_winner"],
                    m["toss_decision"],
                    m["match_winner"],
                    m["win_by_runs"],
                    m["win_by_wickets"],
                    m["player_of_match"],
                    m["event_name"],
                    m["event_stage"],
                    m["event_match_number"],
                )
                for m in new_matches
            ]
            cursor.executemany(
                """
                INSERT INTO RAW.RAW_MATCHES (
                    match_id, match_date, season, team1, team2, venue, city,
                    toss_winner, toss_decision, match_winner, win_by_runs,
                    win_by_wickets, player_of_match, event_name, event_stage,
                    event_match_number
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )

        logger.info("Matches inserted: %d, skipped: %d", len(new_matches), skipped)
    finally:
        cursor.close()


def load_deliveries(conn, deliveries):
    """Delete and reload delivery records for all match_ids in the input list.

    Performs a clean reload per match: existing rows for each match_id are
    deleted before the new rows are inserted.

    Args:
        conn: An open Snowflake connection.
        deliveries: List of delivery dicts from parse_deliveries().
    """
    if not deliveries:
        logger.info("No deliveries to load")
        return

    cursor = conn.cursor()
    try:
        match_ids = list({d["match_id"] for d in deliveries})
        placeholders = ", ".join(["%s"] * len(match_ids))
        cursor.execute(
            f"DELETE FROM RAW.RAW_DELIVERIES WHERE match_id IN ({placeholders})",
            match_ids,
        )
        logger.info("Deleted existing deliveries for %d matches", len(match_ids))

        rows = [
            (
                d["match_id"],
                d["inning_number"],
                d["batting_team"],
                d["over_number"],
                d["ball_number"],
                d["batter"],
                d["bowler"],
                d["non_striker"],
                d["runs_batter"],
                d["runs_extras"],
                d["runs_total"],
                d["extras_type"],
                d["is_wicket"],
                d["wicket_kind"],
                d["player_out"],
                d["fielder"],
            )
            for d in deliveries
        ]
        insert_sql = """
            INSERT INTO RAW.RAW_DELIVERIES (
                match_id, inning_number, batting_team, over_number, ball_number,
                batter, bowler, non_striker, runs_batter, runs_extras, runs_total,
                extras_type, is_wicket, wicket_kind, player_out, fielder
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        BATCH_SIZE = 10_000
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            logger.info("Inserted deliveries batch %d/%d",
                        min(i + BATCH_SIZE, len(rows)), len(rows))
        logger.info("Deliveries inserted: %d", len(rows))
    finally:
        cursor.close()


if __name__ == "__main__":
    env = load_env()
    all_matches, all_deliveries = parse_all_files(env["DATA_FOLDER"])

    conn = get_snowflake_connection()
    try:
        create_raw_tables(conn)
        load_matches(conn, all_matches)
        load_deliveries(conn, all_deliveries)
        print(f"Matches loaded:    {len(all_matches)}")
        print(f"Deliveries loaded: {len(all_deliveries)}")
    finally:
        conn.close()
        logger.info("Snowflake connection closed")
