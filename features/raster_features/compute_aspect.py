import os
import sys
import subprocess
import warnings
from osgeo import gdal, osr
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")


def safe_gdal_edit_nodata(path, nodata_value=0):
    """
    Safely set NoData value using gdal_edit, cross-platform compatible.
    """
    try:
        subprocess.run(
            [sys.executable, "-m", "osgeo_utils.gdal_edit", "-a_nodata", str(nodata_value), path],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
    except subprocess.CalledProcessError as e:
        print(f"[WARN] Failed to set NoData={nodata_value} for {path}: {e.stderr.decode().strip()}")


def compute_aspect(config: str, client_id: str, artifact_url: str,
                   store_artifact: str, file_path: str = None) -> None:
    """
    Function to compute aspect from a DEM (COG or regular GeoTIFF) using GDAL's gdaldem. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-aspect.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    # --- Step 1: Connect to MinIO and download DEM ---
    client = connect_minio(config, client_id)

    temp_input = "temp_dem.tif"
    temp_aspect_raw = "temp_aspect_raw.tif"
    temp_aspect_cog = "temp_aspect_cog.tif"

    with client.get_object(client_id, artifact_url) as response:
        dem_data = response.read()
    with open(temp_input, "wb") as f:
        f.write(dem_data)

    # --- Step 2: Ensure DEM has valid projection & NoData ---
    dem_ds = gdal.Open(temp_input, gdal.GA_ReadOnly)
    if dem_ds is None:
        raise FileNotFoundError(f"Could not open DEM: {temp_input}")

    srs = osr.SpatialReference(wkt=dem_ds.GetProjection())
    if srs.IsProjected() == 0:
        print("[INFO] DEM is not projected; reprojecting to EPSG:4326.")
        reprojected_path = "temp_dem_projected.tif"
        gdal.Warp(reprojected_path, temp_input, dstSRS="EPSG:4326")
        dem_ds = None
        os.remove(temp_input)
        temp_input = reprojected_path

    # --- Step 3: Ensure DEM has NoData value ---
    band = dem_ds.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    dem_ds = None
    if nodata is None:
        print("[INFO] DEM has no NoData value; setting NoData=0.")
        safe_gdal_edit_nodata(temp_input, 0)

    # --- Step 4: Compute Aspect using GDAL DEMProcessing ---
    gdal.DEMProcessing(temp_aspect_raw, temp_input, "aspect", computeEdges=True)

    # --- Step 5: Convert to COG ---
    try:
        tiff_to_cogtiff(temp_aspect_raw, temp_aspect_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert aspect TIFF to COG: {e}")

    # --- Step 6: Save artifact ---
    if store_artifact:
        save_raster_artifact(
            config=config,
            client_id=client_id,
            local_path=temp_aspect_cog,
            file_path=file_path,
            store_artifact=store_artifact
        )
        print(f"{file_path}")
    else:
        print("[INFO] Aspect computed but not saved. Set store_artifact to 'minio' or 'local'.")

    # --- Step 7: Cleanup temporary files ---
    for fpath in [temp_input, temp_aspect_raw, temp_aspect_cog]:
        try:
            if os.path.exists(fpath):
                os.remove(fpath)
        except Exception:
            pass