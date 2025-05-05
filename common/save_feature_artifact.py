import os
import uuid
import warnings
import subprocess
import geopandas as gpd
from common.minio_ops import connect_store_minio

warnings.filterwarnings("ignore")

def reproject_with_ogr(input_path, output_path, target_epsg="4326"):
    """Reproject a GeoJSON file using ogr2ogr."""
    try:
        command = [
            "ogr2ogr",
            "-f", "GeoJSON",
            "-t_srs", f"EPSG:{target_epsg}",
            output_path,
            input_path
        ]
        subprocess.check_call(command)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"[ERROR] ogr2ogr reprojection failed: {e}")


def save_feature(client_id, gdf, file_path, config_path, store_artifact):
    """
    Save a GeoDataFrame after ensuring it is projected to EPSG:4326 using ogr2ogr.
    """
    # Step 1: Save the input GeoDataFrame temporarily
    temp_input = "temp_input.geojson"
    gdf.to_file(temp_input, driver="GeoJSON")

    # Step 2: Reproject using ogr2ogr
    temp_output = "temp_output.geojson"
    reproject_with_ogr(temp_input, temp_output, target_epsg="4326")

    # Step 3: Load the reprojected GeoJSON back (optional, only if you want to do further processing)
    reprojected_gdf = gpd.read_file(temp_output)

    # Step 4: Save as final file
    if not file_path:
        file_path = f"{uuid.uuid4()}.geojson"

    if store_artifact.lower() == "minio":
        try:
            connect_store_minio(config_path, client_id, temp_output, file_path)
        except Exception as e:
            raise Exception(f"[ERROR] Error while saving to MinIO: {e}")
    elif store_artifact.lower() == "local":
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            reprojected_gdf.to_file(file_path, driver="GeoJSON")
            print(f"[INFO] Data saved locally to {file_path}")
        except Exception as e:
            raise Exception(f"[ERROR] Error while saving locally: {e}")
    else:
        raise ValueError("[ERROR] Invalid value for store_artifact. Use either 'local' or 'minio'.")

    # Step 5: Clean up temp files
    os.remove(temp_input)
    os.remove(temp_output)
