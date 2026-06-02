import json
from pathlib import Path

from ingestion.utils import get_logger, load_env

logger = get_logger(__name__)


def parse_match_info(file_path, match_id):
    """Parse top-level match metadata from a Cricsheet JSON file.

    Args:
        file_path: Path to the JSON file.
        match_id: Identifier derived from the filename stem.

    Returns:
        A dict with one row of match-level fields.
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    info = data["info"]
    outcome = info.get("outcome", {})
    outcome_by = outcome.get("by", {})
    event = info.get("event", {})
    player_of_match = info.get("player_of_match") or []
    teams = info.get("teams", [])

    return {
        "match_id": match_id,
        "match_date": (info.get("dates") or [None])[0],
        "season": str(info.get("season", "")),
        "team1": teams[0] if len(teams) > 0 else None,
        "team2": teams[1] if len(teams) > 1 else None,
        "venue": info.get("venue"),
        "city": info.get("city"),
        "toss_winner": info.get("toss", {}).get("winner"),
        "toss_decision": info.get("toss", {}).get("decision"),
        "match_winner": outcome.get("winner"),
        "win_by_runs": outcome_by.get("runs"),
        "win_by_wickets": outcome_by.get("wickets"),
        "player_of_match": player_of_match[0] if player_of_match else None,
        "event_name": event.get("name"),
        "event_stage": event.get("stage"),
        "event_match_number": event.get("match_number"),
    }


def parse_deliveries(file_path, match_id):
    """Parse every delivery from a Cricsheet JSON file into a flat list of dicts.

    Args:
        file_path: Path to the JSON file.
        match_id: Identifier derived from the filename stem.

    Returns:
        A list of dicts, one per delivery across all innings.
    """
    with open(file_path, "r") as f:
        data = json.load(f)

    rows = []
    for inning_idx, inning in enumerate(data.get("innings", [])):
        inning_number = inning_idx + 1
        batting_team = inning.get("team")

        for over_obj in inning.get("overs", []):
            over_number = over_obj.get("over")

            for ball_number, delivery in enumerate(over_obj.get("deliveries", [])):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras", {})
                wickets = delivery.get("wickets")

                extras_type = next(iter(extras), None) if extras else None

                is_wicket = bool(wickets)
                wicket_kind = None
                player_out = None
                fielder = None

                if wickets:
                    first_wicket = wickets[0]
                    wicket_kind = first_wicket.get("kind")
                    player_out = first_wicket.get("player_out")
                    fielders = first_wicket.get("fielders", [])
                    fielder = fielders[0]["name"] if fielders else None

                rows.append({
                    "match_id": match_id,
                    "inning_number": inning_number,
                    "batting_team": batting_team,
                    "over_number": over_number,
                    "ball_number": ball_number,
                    "batter": delivery.get("batter"),
                    "bowler": delivery.get("bowler"),
                    "non_striker": delivery.get("non_striker"),
                    "runs_batter": runs.get("batter", 0),
                    "runs_extras": runs.get("extras", 0),
                    "runs_total": runs.get("total", 0),
                    "extras_type": extras_type,
                    "is_wicket": is_wicket,
                    "wicket_kind": wicket_kind,
                    "player_out": player_out,
                    "fielder": fielder,
                })

    return rows


def parse_all_files(data_folder):
    """Parse all Cricsheet JSON files in a folder.

    Args:
        data_folder: Path string to the folder containing .json files.

    Returns:
        Tuple of (all_matches, all_deliveries) — both lists of dicts.
    """
    folder = Path(data_folder)
    json_files = sorted(folder.glob("*.json"))
    total = len(json_files)

    all_matches = []
    all_deliveries = []

    for idx, file_path in enumerate(json_files, start=1):
        match_id = file_path.stem
        try:
            all_matches.append(parse_match_info(file_path, match_id))
            all_deliveries.extend(parse_deliveries(file_path, match_id))
        except Exception as exc:
            logger.error("Failed to parse %s: %s", file_path.name, exc)
            continue

        if idx % 100 == 0:
            logger.info("Processed %d/%d files", idx, total)

    return all_matches, all_deliveries


if __name__ == "__main__":
    env = load_env()
    all_matches, all_deliveries = parse_all_files(env["DATA_FOLDER"])
    print(f"Total matches parsed:    {len(all_matches)}")
    print(f"Total deliveries parsed: {len(all_deliveries)}")
