import os
import uuid
import tempfile
import shutil
import warnings
from osgeo import gdal, osr
from common.minio_ops import connect_minio, stream_to_minio
from common.convert_to_cog import tiff_to_cogtiff

warnings.filterwarnings("ignore")

def reproject_to_epsg4326(input_path: str, output_path: str):
    """
    Reprojects a raster to EPSG:4326 and writes to output_path (GeoTIFF).
    Handles cases with unknown or non-standard input CRS.
    """
    src_ds = gdal.Open(input_path)
    if not src_ds:
        raise RuntimeError(f"Could not open raster: {input_path}")

    src_srs_wkt = src_ds.GetProjection()
    if not src_srs_wkt:
        raise RuntimeError("Source raster has no defined projection.")

    # Define target SRS
    dst_srs = osr.SpatialReference()
    dst_srs.ImportFromEPSG(4326)

    try:
        warp_options = gdal.WarpOptions(dstSRS=dst_srs.ExportToWkt(), format='GTiff')
        gdal.Warp(destNameOrDestDS=output_path, srcDSOrSrcDSTab=src_ds, options=warp_options)
    except Exception as e:
        raise RuntimeError(f"GDAL Warp failed: {e}")


def save_raster_artifact(config: str, client_id: str, local_path: str, file_path: str, store_artifact: str):
    """
    Reprojects raster to EPSG:4326, converts to LZW-compressed COG, and saves to MinIO or local.
    """
    if not file_path:
        file_path = f"processed_rasters/{uuid.uuid4()}.tif"

    with tempfile.TemporaryDirectory() as tmpdir:
        raw_reprojected = os.path.join(tmpdir, "reprojected.tif")
        cog_path = os.path.join(tmpdir, "final_cog.tif")

        # Step 1: Reproject
        reproject_to_epsg4326(local_path, raw_reprojected)

        # Step 2: Convert to LZW COG using existing utility
        tiff_to_cogtiff(raw_reprojected, cog_path)

        # Step 3: Upload to minio or save locally
        if store_artifact.lower() == "minio":
            try:
                client = connect_minio(config, client_id)
                stream_to_minio(client, client_id, file_path, cog_path)

                aux_path = cog_path + ".aux.xml"
                if os.path.exists(aux_path):
                    stream_to_minio(client, client_id, file_path + ".aux.xml", aux_path)
            except Exception as e:
                raise Exception(f"Error while saving raster to MinIO: {e}")

        elif store_artifact.lower() == "local":
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                shutil.move(cog_path, file_path)
                print(f"Raster saved locally at: {file_path}")

                aux_path = cog_path + ".aux.xml"
                if os.path.exists(aux_path):
                    shutil.move(aux_path, file_path + ".aux.xml")
                    print(f"Auxiliary XML saved locally at: {file_path}.aux.xml")

            except Exception as e:
                raise Exception(f"Error while saving raster locally: {e}")

        else:
            raise ValueError("Invalid store_artifact. Use either 'minio' or 'local'.")
