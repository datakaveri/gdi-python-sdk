import os
import uuid
from common.minio_ops import connect_minio, stream_to_minio
import warnings
warnings.filterwarnings("ignore")

def save_raster_artifact(config: str, client_id: str, local_path: str, file_path: str, store_artifact: str):
    """
    Saves a raster file to MinIO or local storage. If an aux.xml exists, it uploads that as well.

    """
    if not file_path:
        file_path = f"processed_rasters/{uuid.uuid4()}.tif"

    if store_artifact.lower() == "minio":
        try:
            client = connect_minio(config, client_id)
            stream_to_minio(client, client_id, file_path, local_path)

            # Check for aux.xml and upload if exists
            aux_path = local_path + ".aux.xml"
            if os.path.exists(aux_path):
                aux_key = file_path + ".aux.xml"
                stream_to_minio(client, client_id, aux_key, aux_path)

        except Exception as e:
            raise Exception(f"Error while saving raster to MinIO: {e}")

    elif store_artifact.lower() == "local":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            os.rename(local_path, file_path)
            print(f"Raster saved locally at: {file_path}")

            # Save aux.xml if exists
            aux_path = local_path + ".aux.xml"
            aux_target = file_path + ".aux.xml"
            if os.path.exists(aux_path):
                os.rename(aux_path, aux_target)
                print(f"Auxiliary XML saved locally at: {aux_target}")

        except Exception as e:
            raise Exception(f"Error while saving raster locally: {e}")

    else:
        raise ValueError("Invalid store_artifact. Use either 'minio' or 'local'.")
