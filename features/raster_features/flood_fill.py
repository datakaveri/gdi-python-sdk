import os
import numpy as np
import tempfile
import warnings
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")

def flood_fill(
    config: str,
    client_id: str,
    artifact_url: str,
    threshold: float,
    store_artifact: str,
    file_path: str = None
) -> str:
    """
    Generate flood inundated raster based on DEM read from MinIO and input threshold value. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as flood-fill-model.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    threshold : float (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter, This parameter will be optoinal)
    """

    client = connect_minio(config, client_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        dem_path = os.path.join(tmpdir, "dem.tif")
        flood_raw = os.path.join(tmpdir, "flood_raw.tif")
        flood_cog = os.path.join(tmpdir, "flood_cog.tif")

        # Download DEM from MinIO
        try:
            with client.get_object(client_id, artifact_url) as response:
                with open(dem_path, "wb") as f:
                    f.write(response.read())
        except Exception as e:
            raise RuntimeError(f"[ERROR] Failed to download DEM from MinIO: {e}")

        try:
            # Open DEM and read into numpy array
            ds = gdal.Open(dem_path)
            if ds is None:
                raise RuntimeError("Failed to open DEM raster with GDAL.")
            band = ds.GetRasterBand(1)
            array = band.ReadAsArray().astype(np.float32)
            nodata = band.GetNoDataValue()
            if nodata is not None:
                array[array == nodata] = np.nan

            # Validate threshold
            min_elev, max_elev = np.nanmin(array), np.nanmax(array)
            if not (min_elev - 1 <= threshold <= max_elev + 1):
                raise ValueError(f"Threshold {threshold} out of range ({min_elev}, {max_elev})")

            # Generate binary mask
            binary_mask = np.where(array < threshold, 1, 0)
            sy, sx = np.unravel_index(np.nanargmin(array), array.shape)

            flood = np.zeros_like(binary_mask, dtype=np.uint8)
            fill = {(sx, sy)}
            filled = set()
            height, width = binary_mask.shape

            while fill:
                x, y = fill.pop()
                if 0 <= x < width and 0 <= y < height and binary_mask[y, x] == 1:
                    flood[y, x] = 1
                    filled.add((x, y))
                    fill.update({
                        (x - 1, y), (x + 1, y),
                        (x, y - 1), (x, y + 1)
                    } - filled)

            # Save flood result using GDAL
            driver = gdal.GetDriverByName("GTiff")
            out_ds = driver.Create(flood_raw, width, height, 1, gdal.GDT_Byte)
            out_ds.SetGeoTransform(ds.GetGeoTransform())
            out_ds.SetProjection(ds.GetProjection())
            out_ds.GetRasterBand(1).WriteArray(flood)
            out_ds.GetRasterBand(1).SetNoDataValue(0)
            out_ds.FlushCache()
            out_ds = None
            ds = None  # Close input raster

            # Convert to COG
            tiff_to_cogtiff(flood_raw, flood_cog)

            # Save via save_raster_artifact
            save_raster_artifact(
                config=config,
                client_id=client_id,
                local_path=flood_cog,
                file_path=file_path,
                store_artifact=store_artifact
            )
            print(f"{file_path}")
        except Exception as e:
            raise RuntimeError(f"[ERROR] Flood fill failed: {e}")