import os
import subprocess
import warnings
from osgeo import gdal, osr
from common.minio_ops import connect_minio, get_bucket_name
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")


def compute_hillshade(
    config: str,
    artifact_url: str,
    store_artifact: str,
    file_path: str = None,
) -> None:
    """
    Function to compute hillshade from a DEM (COG or regular GeoTIFF) using GDAL's gdaldem. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-hillshade.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config)
    bucket_name = get_bucket_name(config)

    temp_dem = "temp_dem.tif"
    temp_dem_7755 = "temp_dem_7755.tif"
    temp_hillshade_raw = "temp_hillshade_raw.tif"
    temp_hillshade_cog = "temp_hillshade_cog.tif"

    # Download DEM
    with client.get_object(bucket_name, artifact_url) as response:
        dem_data = response.read()
    with open(temp_dem, "wb") as f:
        f.write(dem_data)

    dem_for_hillshade = temp_dem

    # Check and reproject CRS if needed
    src_ds = gdal.Open(temp_dem)
    src_srs = osr.SpatialReference()
    src_srs.ImportFromWkt(src_ds.GetProjection())
    target_srs = osr.SpatialReference()
    target_srs.ImportFromEPSG(7755)

    if not src_srs.IsSame(target_srs):
        warp_options = gdal.WarpOptions(dstSRS="EPSG:7755", resampleAlg="bilinear")
        gdal.Warp(temp_dem_7755, src_ds, options=warp_options)
        dem_for_hillshade = temp_dem_7755
    src_ds = None

    # Ensure NoData=0
    ds = gdal.Open(dem_for_hillshade)
    band = ds.GetRasterBand(1)
    nodata = band.GetNoDataValue()
    if nodata is None or nodata != 0:
        subprocess.run(
            ["gdal_edit.py", "-a_nodata", "0", dem_for_hillshade], check=True
        )
    ds = None

    # Compute hillshade
    gdal_hillshade_cmd = [
        "gdaldem",
        "hillshade",
        dem_for_hillshade,
        temp_hillshade_raw,
        "-s",
        "1",
        "-compute_edges",
        "-of",
        "GTiff",
    ]

    try:
        subprocess.run(
            gdal_hillshade_cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"[ERROR] gdaldem hillshade failed: {e}")

    # Convert to COG
    try:
        tiff_to_cogtiff(temp_hillshade_raw, temp_hillshade_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert raw hillshade TIF to COG: {e}")

    # Save to MinIO or local
    if store_artifact:
        save_raster_artifact(
            config=config,
            local_path=temp_hillshade_cog,
            file_path=file_path,
            store_artifact=store_artifact,
        )
        # print(f"{file_path}")
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print("Hillshade computed successfully.")

    # Cleanup
    try:
        for fpath in [temp_dem, temp_dem_7755, temp_hillshade_raw, temp_hillshade_cog]:
            if os.path.exists(fpath):
                os.remove(fpath)

    except Exception as e:
        print(f"[WARN] Failed to clean up intermediate files: {e}")
