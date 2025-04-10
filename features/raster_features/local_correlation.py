import os
import warnings
import numpy as np
import numba
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")

@numba.njit
def local_corr_numba(x_win, y_win):
    n = 0
    sum_x = 0.0
    sum_y = 0.0
    sum_xy = 0.0
    sum_x2 = 0.0
    sum_y2 = 0.0
    for k in range(x_win.size):
        x = x_win[k]
        y = y_win[k]
        if np.isnan(x) or np.isnan(y):
            continue
        n += 1
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_x2 += x * x
        sum_y2 += y * y
    if n < 2:
        return np.nan
    numerator = n * sum_xy - sum_x * sum_y
    denominator = np.sqrt((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y))
    if denominator == 0:
        return np.nan
    return numerator / denominator

@numba.njit
def compute_corr_chunk(x_chunk, y_chunk, pad):
    height, width = x_chunk.shape
    corr_chunk = np.full((height - 2 * pad, width - 2 * pad), np.nan, dtype=np.float32)
    for i in range(pad, height - pad):
        for j in range(pad, width - pad):
            x_win = x_chunk[i - pad:i + pad + 1, j - pad:j + pad + 1].ravel()
            y_win = y_chunk[i - pad:i + pad + 1, j - pad:j + pad + 1].ravel()
            corr_chunk[i - pad, j - pad] = local_corr_numba(x_win, y_win)
    return corr_chunk

def compute_local_correlation_5x5(
    config: str,
    client_id: str,
    x: str,
    y: str,
    chunk_size: int,
    store_artifact: str,
    file_path: str = None,
) -> str:
    """
    Compute local (5x5) correlation between two rasters. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-ndvi.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    x : str (Reactflow will take it from the previous step)
    y : str (Reactflow will take it from the previous step)
    chunk_size : int (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    window_size = 5
    pad = window_size // 2

    client = connect_minio(config, client_id)

    # Temporary file paths
    temp_x = "temp_x.tif"
    temp_y = "temp_y.tif"
    temp_y_aligned = "temp_y_aligned.tif"
    temp_corr = "temp_corr.tif"
    temp_corr_cog = "temp_corr_cog.tif"

    # Download input rasters from MinIO
    with client.get_object(client_id, x) as response:
        with open(temp_x, "wb") as f:
            f.write(response.read())
    with client.get_object(client_id, y) as response:
        with open(temp_y, "wb") as f:
            f.write(response.read())

    # Reading raster X and Y
    x_ds = gdal.Open(temp_x)
    y_ds = gdal.Open(temp_y)

    # Aligning raster Y to raster X
    gt = x_ds.GetGeoTransform()
    minx = gt[0]
    maxy = gt[3]
    maxx = minx + gt[1] * x_ds.RasterXSize
    miny = maxy + gt[5] * x_ds.RasterYSize

    warp_options = gdal.WarpOptions(
        format='GTiff',
        width=x_ds.RasterXSize,
        height=x_ds.RasterYSize,
        dstSRS=x_ds.GetProjection(),
        outputBounds=(minx, miny, maxx, maxy),
        resampleAlg=gdal.GRA_Bilinear
    )
    gdal.Warp(temp_y_aligned, y_ds, options=warp_options)

    y_aligned_ds = gdal.Open(temp_y_aligned)

    # Creating output raster
    driver = gdal.GetDriverByName("GTiff")
    corr_ds = driver.Create(
        temp_corr,
        x_ds.RasterXSize,
        x_ds.RasterYSize,
        1,
        gdal.GDT_Float32
    )
    corr_ds.SetGeoTransform(x_ds.GetGeoTransform())
    corr_ds.SetProjection(x_ds.GetProjection())
    corr_ds.GetRasterBand(1).SetNoDataValue(-9999.0)
    corr_ds.GetRasterBand(1).Fill(-9999.0)
    corr_ds.FlushCache()

    # Chunk-wise correlation computation
    x_band = x_ds.GetRasterBand(1)
    y_band = y_aligned_ds.GetRasterBand(1)

    for y_off in range(0, x_ds.RasterYSize, chunk_size):
        for x_off in range(0, x_ds.RasterXSize, chunk_size):
            win_xsize = min(chunk_size + 2 * pad, x_ds.RasterXSize - x_off)
            win_ysize = min(chunk_size + 2 * pad, x_ds.RasterYSize - y_off)

            x_chunk = x_band.ReadAsArray(x_off, y_off, win_xsize, win_ysize).astype(np.float32)
            y_chunk = y_band.ReadAsArray(x_off, y_off, win_xsize, win_ysize).astype(np.float32)

            x_chunk[x_chunk == x_band.GetNoDataValue()] = np.nan
            y_chunk[y_chunk == y_band.GetNoDataValue()] = np.nan

            corr_chunk = compute_corr_chunk(x_chunk, y_chunk, pad)
            # Clip correlation values and mask invalid ones
            corr_chunk = np.clip(corr_chunk, -1.0, 1.0)
            corr_chunk[np.isnan(corr_chunk)] = -9999.0

            corr_ds.GetRasterBand(1).WriteArray(corr_chunk, x_off + pad, y_off + pad)

    # Convert result to COG
    tiff_to_cogtiff(temp_corr, temp_corr_cog)

    # Save to local or MinIO
    if store_artifact:
        save_raster_artifact(config, client_id, temp_corr_cog, file_path, store_artifact)
        result_path = file_path
    else:
        result_path = temp_corr_cog

    # Ensure datasets are closed
    x_ds = None
    y_ds = None
    y_aligned_ds = None
    corr_ds = None

    # Clean up temporary files
    for f in [temp_x, temp_y, temp_y_aligned, temp_corr, temp_corr_cog]:
        try:
            if os.path.exists(f):
                os.remove(f)
        except Exception as e:
            print(f"[WARN] Failed to clean up file {f}: {e}")

    return result_path
