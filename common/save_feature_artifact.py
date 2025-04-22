import os 
import geopandas as gpd
from common.minio_ops import connect_store_minio
import uuid
import warnings
warnings.filterwarnings("ignore")

def save_feature(client_id, gdf, file_path, config_path, store_artifact):
    gdf = gdf.to_crs("EPSG:4326")
    if not file_path:
        file_path = f"{uuid.uuid4()}.geojson"

    if store_artifact.lower() == "minio":
        try:
            # Save locally as temp file before uploading
            temp_file = "temp.geojson"
            gdf.to_file(temp_file, driver="GeoJSON")

            # Upload temp file to MinIO
            connect_store_minio(config_path, client_id, temp_file, file_path)

            # Clean up temp file
            os.remove(temp_file)
        except Exception as e:
            raise Exception(f"Error while saving to MinIO: {e}")

    elif store_artifact.lower() == "local":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            gdf.to_file(file_path, driver="GeoJSON")
            print(f"Data saved locally to {file_path}")
        except Exception as e:
            raise Exception(f"Error while saving locally: {e}")
    else:
        print("Invalid value for store_artifact. Use either 'local' or 'minio'.")
