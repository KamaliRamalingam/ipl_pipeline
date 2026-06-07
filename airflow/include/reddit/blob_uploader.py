import json
import os
from datetime import datetime

from azure.storage.blob import BlobServiceClient

from ingestion.utils import get_logger, retry

logger = get_logger(__name__)


@retry(max_attempts=3, delay=2)
def upload_reddit_batch(
    posts: list[dict],
    fetched_at: datetime,
    container_name: str,
    blob_prefix: str,
) -> str:
    """Upload a batch of scored Reddit posts as a JSON file to Azure Blob Storage.

    The blob path encodes the fetch timestamp so each micro-batch lands in its
    own file. If the DAG retries within the same minute the blob is overwritten
    rather than erroring, which keeps reruns idempotent.

    Blob path format:
        {blob_prefix}/{YYYY}/{MM}/{DD}/{HH}/reddit_ipl_{YYYYmmdd_HHMMSS}.json
    Example:
        reddit/raw/2026/06/06/14/reddit_ipl_20260606_143000.json

    Args:
        posts: List of scored post dicts to serialise and upload.
        fetched_at: Datetime of the fetch run, used to build the blob path.
        container_name: Azure Blob container name.
        blob_prefix: Path prefix inside the container, e.g. ``"reddit/raw"``.

    Returns:
        The full blob path string. Stored in Snowflake for end-to-end traceability.
    """
    blob_path = (
        f"{blob_prefix}/{fetched_at.strftime('%Y/%m/%d/%H')}/"
        f"reddit_ipl_{fetched_at.strftime('%Y%m%d_%H%M%S')}.json"
    )

    data = json.dumps(posts, default=str, indent=2)

    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_path
    )
    blob_client.upload_blob(data, overwrite=True)

    logger.info("Uploaded Reddit batch to blob: %s", blob_path)
    return blob_path
