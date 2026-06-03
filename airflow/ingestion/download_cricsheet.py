# ingestion/download_cricsheet.py

import io
import zipfile
import requests
from pathlib import Path
from ingestion.utils import get_logger

logger = get_logger(__name__)

CRICSHEET_IPL_URL = "https://cricsheet.org/downloads/ipl_json.zip"


def download_from_cricsheet(data_folder: str) -> int:
    """Download the latest IPL JSON ZIP from Cricsheet and extract to data_folder.

    Skips files that already exist — so only new match files are written.

    Args:
        data_folder: Local path where JSON files will be extracted.

    Returns:
        Number of new files extracted.
    """
    folder = Path(data_folder)
    folder.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading IPL data from Cricsheet...")
    response = requests.get(CRICSHEET_IPL_URL, timeout=120)
    response.raise_for_status()
    logger.info("Download complete — %.1f MB", len(response.content) / 1_000_000)

    new_files = 0
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        json_files = [f for f in zf.namelist() if f.endswith(".json")]
        logger.info("ZIP contains %d JSON files", len(json_files))

        for filename in json_files:
            target_path = folder / Path(filename).name

            # Skip if already downloaded — incremental behaviour
            if target_path.exists():
                continue

            with zf.open(filename) as src, open(target_path, "wb") as dst:
                dst.write(src.read())
            new_files += 1

    logger.info("New files extracted: %d", new_files)
    return new_files


if __name__ == "__main__":
    from ingestion.utils import load_env
    env = load_env()
    count = download_from_cricsheet(env["DATA_FOLDER"])
    print(f"Extracted {count} new files")