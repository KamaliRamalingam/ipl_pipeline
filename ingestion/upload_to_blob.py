import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from ingestion.utils import get_logger, load_env, retry

logger = get_logger(__name__)


def get_blob_client():
    """Return an Azure BlobServiceClient using the connection string from the environment.

    Returns:
        A BlobServiceClient instance.
    """
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    return BlobServiceClient.from_connection_string(connection_string)


@retry(max_attempts=3, delay=2)
def upload_file(file_path, container_name, blob_path):
    """Upload a single file to Azure Blob Storage, skipping if the blob already exists.

    Args:
        file_path: Local path to the file to upload.
        container_name: Name of the Azure Blob container.
        blob_path: Destination blob path inside the container (e.g. 'ipl/1082591.json').

    Returns:
        'uploaded' if the file was uploaded, 'skipped' if the blob already existed.
    """
    client = get_blob_client()
    blob_client = client.get_blob_client(container=container_name, blob=blob_path)

    try:
        blob_client.get_blob_properties()
        logger.info("Skipping %s — blob already exists", blob_path)
        return "skipped"
    except ResourceNotFoundError:
        pass

    with open(file_path, "rb") as f:
        blob_client.upload_blob(f)

    logger.info("Uploaded %s", Path(file_path).name)
    return "uploaded"


def upload_all_files(data_folder, container_name):
    """Upload all JSON files in data_folder to Azure Blob Storage under the 'ipl/' prefix.

    Args:
        data_folder: Path string to the local folder containing .json files.
        container_name: Name of the Azure Blob container.

    Returns:
        Tuple of (uploaded_count, skipped_count).
    """
    folder = Path(data_folder)
    json_files = sorted(folder.glob("*.json"))
    total = len(json_files)

    uploaded_count = 0
    skipped_count = 0

    for idx, file_path in enumerate(json_files, start=1):
        blob_path = f"ipl/{file_path.name}"
        result = upload_file(file_path, container_name, blob_path)
        if result == "uploaded":
            uploaded_count += 1
        else:
            skipped_count += 1

        if idx % 100 == 0:
            logger.info("Processed %d/%d files", idx, total)

    return uploaded_count, skipped_count


if __name__ == "__main__":
    env = load_env()
    uploaded, skipped = upload_all_files(env["DATA_FOLDER"], env["AZURE_CONTAINER_NAME"])
    print(f"Upload complete — uploaded: {uploaded}, skipped: {skipped}")
