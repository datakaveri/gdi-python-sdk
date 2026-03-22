import geopandas as gpd
from common.minio_ops import connect_minio, get_bucket_name
import warnings

warnings.filterwarnings("ignore")
import os
import io
from datetime import timedelta
import uuid


def download_rasters_artifact(config: str, artifact_url: str) -> str:
    """
    Download features from the minio bucket and save it as a geopackage file.In editor it will be renamed as download-raster.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    artifact_url : str (Reactflow will take it from the previous step)
    """

    client = connect_minio(config)
    bucket_name = get_bucket_name(config)

    try:
        file_paths = [fp.strip() for fp in artifact_url.split("$") if fp.strip()]
        for file_path in file_paths:
            pre_signed_url = client.get_presigned_url(
                "GET", bucket_name, file_path, expires=timedelta(days=7)
            )
            print(pre_signed_url)
    except Exception as e:
        raise e


# download_rasters_artifact(config = 'C:/Users/Linda/python-notebooks/OPEN_EO_ANALYTICS/gdi-python-sdk/config.json', client_id = 'c669d152-592d-4a1f-bc98-b5b73111368e', artifact_url = 'downloaded_from_stac/Digital Elevation Model (DEM) at 50 K, Varanasi/TIF_cog.tif$downloaded_from_stac/63K Digital Elevation Model (DEM) at 250 K, Varanasi , Uttar Pradesh/TIF_cog.tif$downloaded_from_stac/63N Digital Elevation Model (DEM) at 250 K, Varanasi , Uttar Pradesh/TIF_cog.tif$downloaded_from_stac/63O Digital Elevation Model (DEM) at 250 K, Varanasi , Uttar Pradesh/TIF_cog.tif')
# download_rasters_artifact(config = 'C:/Users/Linda/python-notebooks/OPEN_EO_ANALYTICS/gdi-python-sdk/config.json', client_id = 'c669d152-592d-4a1f-bc98-b5b73111368e', artifact_url = 'downloaded_from_stac/Digital Elevation Model (DEM) at 50 K, Varanasi/TIF_cog.tif')
