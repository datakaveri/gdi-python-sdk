import os
import uuid
import shutil
import warnings
from common.minio_ops import connect_minio, stream_to_minio

warnings.filterwarnings("ignore")


def save_csv_artifact(
        config: str,
        client_id: str,
        local_path: str,
        file_path: str,
        store_artifact: str
    ):
    """
    Save CSV file to MinIO or local.
    """

    if not file_path:
        file_path = f"processed_csv/{uuid.uuid4()}.csv"

    # ---------------------------------------
    # Save to MinIO
    # ---------------------------------------
    if store_artifact.lower() == "minio":
        try:
            client = connect_minio(config, client_id)
            stream_to_minio(client, client_id, file_path, local_path)
            print(f"{file_path}")
        except Exception as e:
            raise Exception(f"Error while saving CSV to MinIO: {e}")

    # ---------------------------------------
    # Save Locally
    # ---------------------------------------
    elif store_artifact.lower() == "local":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            shutil.move(local_path, file_path)
            print(f"CSV saved locally at: {file_path}")
        except Exception as e:
            raise Exception(f"Error while saving CSV locally: {e}")

    else:
        raise ValueError("Invalid store_artifact. Use either 'minio' or 'local'.")
