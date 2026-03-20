import os
import uuid
import shutil
import warnings
from common.minio_ops import connect_minio, stream_to_minio, get_bucket_name

warnings.filterwarnings("ignore")


def save_raster_artifact(
    config: str, local_path: str, file_path: str, store_artifact: str
):
    """
    Saves raster to MinIO or local and returns the final saved path/key.
    """
    if not file_path:
        file_path = f"processed_rasters/{uuid.uuid4()}.tif"

    if store_artifact.lower() == "minio":
        try:
            client = connect_minio(config)
            bucket_name = get_bucket_name(config)
            stream_to_minio(client, bucket_name, file_path, local_path)

            aux_path = local_path + ".aux.xml"
            if os.path.exists(aux_path):
                stream_to_minio(client, bucket_name, file_path + ".aux.xml", aux_path)

            return file_path

        except Exception as e:
            raise Exception(f"Error while saving raster to MinIO: {e}")

    elif store_artifact.lower() == "local":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            shutil.move(local_path, file_path)

            aux_path = local_path + ".aux.xml"
            if os.path.exists(aux_path):
                shutil.move(aux_path, file_path + ".aux.xml")

            return file_path

        except Exception as e:
            raise Exception(f"Error while saving raster locally: {e}")

    else:
        raise ValueError("Invalid store_artifact. Use either 'minio' or 'local'.")


# import os
# import uuid
# import shutil
# import warnings
# from common.minio_ops import connect_minio, stream_to_minio, get_bucket_name

# warnings.filterwarnings("ignore")


# def save_raster_artifact(
#     config: str, local_path: str, file_path: str, store_artifact: str
# ):
#     """
#     Reprojects raster to EPSG:4326, converts to LZW-compressed COG, and saves to MinIO or local.
#     """
#     if not file_path:
#         file_path = f"processed_rasters/{uuid.uuid4()}.tif"

#     # Upload to minio or save locally
#     if store_artifact.lower() == "minio":
#         try:
#             client = connect_minio(config)
#             bucket_name = get_bucket_name(config)
#             stream_to_minio(client, bucket_name, file_path, local_path)
#             # print(f"{file_path}")
#             aux_path = local_path + ".aux.xml"
#             if os.path.exists(aux_path):
#                 stream_to_minio(client, bucket_name, file_path + ".aux.xml", aux_path)
#                 # print(f"{file_path}.aux.xml")
#         except Exception as e:
#             raise Exception(f"Error while saving raster to MinIO: {e}")

#     elif store_artifact.lower() == "local":
#         try:
#             os.makedirs(os.path.dirname(file_path), exist_ok=True)
#             shutil.move(local_path, file_path)
#             print(f"Raster saved locally at: {file_path}")

#             aux_path = local_path + ".aux.xml"
#             if os.path.exists(aux_path):
#                 shutil.move(aux_path, file_path + ".aux.xml")
#                 print(f"Auxiliary XML saved locally at: {file_path}.aux.xml")

#         except Exception as e:
#             raise Exception(f"Error while saving raster locally: {e}")

#     else:
#         raise ValueError("Invalid store_artifact. Use either 'minio' or 'local'.")
