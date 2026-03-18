import os
import uuid
import warnings
import geopandas as gpd
from common.minio_ops import connect_store_minio

warnings.filterwarnings("ignore")


def reproject_with_ogr(input_path, output_path, target_epsg="4326"):
    """
    Reproject a GeoJSON file (replacement for ogr2ogr using GeoPandas).
    Keeps the same function name and parameters for compatibility.
    """
    try:
        gdf = gpd.read_file(input_path)

        # Ensure CRS exists
        if gdf.crs is None:
            raise ValueError("[ERROR] Input file has no CRS defined.")

        # Reproject
        gdf = gdf.to_crs(f"EPSG:{target_epsg}")

        # Save output
        gdf.to_file(output_path, driver="GeoJSON")

    except Exception as e:
        raise RuntimeError(f"[ERROR] Reprojection failed: {e}")


def save_feature(gdf, file_path, config_path, store_artifact):
    """
    Save a GeoDataFrame after ensuring it is projected to EPSG:4326.
    """

    temp_input = "temp_input.geojson"
    temp_output = "temp_output.geojson"

    try:
        # Step 1: Save input temporarily
        gdf.to_file(temp_input, driver="GeoJSON")

        # Step 2: Reproject (now using GeoPandas internally)
        reproject_with_ogr(temp_input, temp_output, target_epsg="4326")

        # Step 3: Read reprojected file
        reprojected_gdf = gpd.read_file(temp_output)

        # Step 4: Determine output name
        if not file_path:
            file_path = f"{uuid.uuid4()}.geojson"

        # Step 5: Save artifact
        if store_artifact.lower() == "minio":
            connect_store_minio(config_path, temp_output, file_path)
        elif store_artifact.lower() == "local":
            folder = os.path.dirname(file_path)

            if folder:
                os.makedirs(folder, exist_ok=True)
                save_path = file_path
            else:
                save_path = os.path.join(os.getcwd(), file_path)

            reprojected_gdf.to_file(save_path, driver="GeoJSON")

            print(f"[INFO] Data saved locally to {save_path}")

        else:
            raise ValueError(
                "[ERROR] Invalid value for store_artifact. Use either 'local' or 'minio'."
            )

    except Exception as e:
        raise Exception(f"[ERROR] save_feature failed: {e}")

    finally:
        # Step 6: Cleanup
        if os.path.exists(temp_input):
            os.remove(temp_input)

        if os.path.exists(temp_output):
            os.remove(temp_output)


# import os
# import uuid
# import warnings
# import subprocess
# import geopandas as gpd
# from common.minio_ops import connect_store_minio

# warnings.filterwarnings("ignore")

# def reproject_with_ogr(input_path, output_path, target_epsg="4326"):
#     """Reproject a GeoJSON file using ogr2ogr."""
#     try:
#         command = [
#             "ogr2ogr",
#             "-f", "GeoJSON",
#             "-t_srs", f"EPSG:{target_epsg}",
#             output_path,
#             input_path
#         ]
#         subprocess.check_call(command)

#     except subprocess.CalledProcessError as e:
#         raise RuntimeError(f"[ERROR] ogr2ogr reprojection failed: {e}")


# def save_feature(client_id, gdf, file_path, config_path, store_artifact):
#     """
#     Save a GeoDataFrame after ensuring it is projected to EPSG:4326 using ogr2ogr.
#     """
#     # Step 1: Save the input GeoDataFrame temporarily
#     temp_input = "temp_input.geojson"
#     gdf.to_file(temp_input, driver="GeoJSON")

#     # Step 2: Reproject using ogr2ogr
#     temp_output = "temp_output.geojson"
#     reproject_with_ogr(temp_input, temp_output, target_epsg="4326")

#     # Step 3: Load the reprojected GeoJSON back (optional, only if you want to do further processing)
#     reprojected_gdf = gpd.read_file(temp_output)

#     # Step 4: Save as final file
#     if not file_path:
#         file_path = f"{uuid.uuid4()}.geojson"

#     if store_artifact.lower() == "minio":
#         try:
#             connect_store_minio(config_path, client_id, temp_output, file_path)
#         except Exception as e:
#             raise Exception(f"[ERROR] Error while saving to MinIO: {e}")
#     elif store_artifact.lower() == "local":
#         try:
#             folder = os.path.dirname(file_path)
#             if folder:
#                 os.makedirs(folder, exist_ok=True)
#             save_path = file_path if folder else os.path.join(os.getcwd(), file_path)
#             reprojected_gdf.to_file(save_path, driver="GeoJSON")
#             print(f"[INFO] Data saved locally to {save_path}")
#         except Exception as e:
#             raise Exception(f"[ERROR] Error while saving locally: {e}")

#     else:
#         raise ValueError("[ERROR] Invalid value for store_artifact. Use either 'local' or 'minio'.")

#     # Step 5: Clean up temp files
#     os.remove(temp_input)
#     os.remove(temp_output)
