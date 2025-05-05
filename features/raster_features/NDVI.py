import os
import numpy as np
import warnings
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")

def compute_ndvi(config: str, client_id: str, red_artifact_url: str, nir_artifact_url: str, store_artifact: str, file_path: str = None) -> None:
    """
    Function to compute NDVI from Red and NIR bands. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-ndvi.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    red_artifact_url : str (Reactflow will take it from the previous step)
    nir_artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)

    temp_red = "temp_red.tif"
    temp_nir = "temp_nir.tif"
    temp_ndvi = "temp_ndvi.tif"
    temp_ndvi_cog = "temp_ndvi_cog.tif"

    try:
        with client.get_object(client_id, red_artifact_url) as response:
            red_data = response.read()
        with open(temp_red, "wb") as f:
            f.write(red_data)

        with client.get_object(client_id, nir_artifact_url) as response:
            nir_data = response.read()
        with open(temp_nir, "wb") as f:
            f.write(nir_data)

    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to download bands from MinIO: {e}")

    try:
        red_ds = gdal.Open(temp_red)
        nir_ds = gdal.Open(temp_nir)

        if red_ds is None or nir_ds is None:
            raise RuntimeError("[ERROR] GDAL failed to open Red or NIR raster.")

        red_band = red_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)
        nir_band = nir_ds.GetRasterBand(1).ReadAsArray().astype(np.float32)

        np.seterr(divide='ignore', invalid='ignore')
        ndvi = (nir_band - red_band) / (nir_band + red_band)
        ndvi = np.nan_to_num(ndvi, nan=-9999.0)

        driver = gdal.GetDriverByName("GTiff")
        ndvi_ds = driver.Create(temp_ndvi, red_ds.RasterXSize, red_ds.RasterYSize, 1, gdal.GDT_Float32)
        ndvi_ds.SetGeoTransform(red_ds.GetGeoTransform())
        ndvi_ds.SetProjection(red_ds.GetProjection())

        ndvi_band = ndvi_ds.GetRasterBand(1)
        ndvi_band.WriteArray(ndvi)
        ndvi_band.SetNoDataValue(-9999.0)

        ndvi_band.FlushCache()
        ndvi_ds.FlushCache()

        red_ds = None
        nir_ds = None
        ndvi_ds = None

    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to compute NDVI: {e}")

    try:
        tiff_to_cogtiff(temp_ndvi, temp_ndvi_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert raw NDVI TIF to COG: {e}")

    if os.path.exists(temp_ndvi):
        os.remove(temp_ndvi)

    if store_artifact:
        save_raster_artifact(config=config, client_id=client_id, local_path=temp_ndvi_cog, file_path=file_path, store_artifact=store_artifact)
        print(f"{file_path}")
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print("NDVI computed successfully.")

    try:
        if os.path.exists(temp_red):
            os.remove(temp_red)
        if os.path.exists(temp_nir):
            os.remove(temp_nir)
        if os.path.exists(temp_ndvi_cog):
            os.remove(temp_ndvi_cog)
    except Exception as e:
        print(f"[WARN] Failed to clean up intermediate files: {e}")
