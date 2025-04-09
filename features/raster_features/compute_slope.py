import os
import subprocess
import warnings
from osgeo import gdal, osr
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")

def compute_slope(config: str, client_id: str, artifact_url: str, store_artifact: str, file_path: str = None) -> None:
    """
    Function to compute slope from a DEM (COG or regular GeoTIFF) using GDAL's gdaldem. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-slope.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)

    temp_dem = "temp_dem.tif"
    temp_dem_7755 = "temp_dem_7755.tif"
    temp_slope_raw = "temp_slope_raw.tif"
    temp_slope_cog = "temp_slope_cog.tif"

    try:
        with client.get_object(client_id, artifact_url) as response:
            dem_data = response.read()
        with open(temp_dem, "wb") as f:
            f.write(dem_data)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to download DEM from MinIO: {e}")

    crs_needs_reproject = False
    dem_for_slope = temp_dem

    try:
        src_ds = gdal.Open(temp_dem)
        if src_ds is None:
            raise RuntimeError("[ERROR] GDAL failed to open DEM for reprojection check.")

        src_wkt = src_ds.GetProjection()
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(src_wkt)

        target_srs = osr.SpatialReference()
        target_srs.ImportFromEPSG(7755)

        if not src_srs.IsSame(target_srs):
            crs_needs_reproject = True
            warp_options = gdal.WarpOptions(dstSRS="EPSG:7755", resampleAlg="bilinear")
            gdal.Warp(temp_dem_7755, src_ds, options=warp_options)
            dem_for_slope = temp_dem_7755

        src_ds = None
    except Exception as e:
        raise RuntimeError(f"[ERROR] GDAL failed to read/reproject DEM: {e}")

    gdal_slope_cmd = [
        "gdaldem", "slope",
        dem_for_slope, temp_slope_raw,
        "-of", "GTiff"
    ]

    try:
        subprocess.run(
            gdal_slope_cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"[ERROR] gdaldem slope failed: {e}")

    try:
        tiff_to_cogtiff(temp_slope_raw, temp_slope_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert raw slope TIF to COG: {e}")

    if os.path.exists(temp_slope_raw):
        os.remove(temp_slope_raw)

    if store_artifact:
        save_raster_artifact(config=config, client_id=client_id, local_path=temp_slope_cog, file_path=file_path, store_artifact=store_artifact)
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print("Slope computed successfully.")
    
    try:
        if os.path.exists(temp_dem):
            os.remove(temp_dem)
        if os.path.exists(temp_slope_cog):
            os.remove(temp_slope_cog)
        if crs_needs_reproject and os.path.exists(temp_dem_7755):
            os.remove(temp_dem_7755)
    
    except Exception as e:
        print(f"[WARN] Failed to clean up intermediate files: {e}")
