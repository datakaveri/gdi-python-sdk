import os
import uuid
import numpy as np
import numba
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact

@numba.njit
def local_corr_numba(dem_win, lst_win):
    n = 0
    sum_x = 0.0
    sum_y = 0.0
    sum_xy = 0.0
    sum_x2 = 0.0
    sum_y2 = 0.0
    for k in range(dem_win.size):
        x = dem_win[k]
        y = lst_win[k]
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
def compute_corr_chunk(dem_chunk, lst_chunk, pad):
    height, width = dem_chunk.shape
    corr_chunk = np.full((height - 2 * pad, width - 2 * pad), np.nan, dtype=np.float64)
    for i in range(pad, height - pad):
        for j in range(pad, width - pad):
            dem_win = dem_chunk[i - pad:i + pad + 1, j - pad:j + pad + 1].ravel()
            lst_win = lst_chunk[i - pad:i + pad + 1, j - pad:j + pad + 1].ravel()
            corr_chunk[i - pad, j - pad] = local_corr_numba(dem_win, lst_win)
    return corr_chunk

def read_array(dataset, xoff, yoff, xsize, ysize, nodata):
    arr = dataset.ReadAsArray(xoff, yoff, xsize, ysize).astype(np.float64)
    arr[arr == nodata] = np.nan
    return arr

def write_array(dataset, arr, xoff, yoff):
    band = dataset.GetRasterBand(1)
    band.WriteArray(arr, xoff, yoff)
    band.FlushCache()


def compute_local_correlation_5x5(config, client_id, dem_artifact_url, lst_artifact_url, chunk_size=500, store_artifact=False, file_path=None):
    """
    Compute local (5x5) correlation between two rasters. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-local-correlation.
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

    temp_dem = "temp_dem.tif"
    temp_lst = "temp_lst.tif"
    aligned_lst = "aligned_lst.tif"
    raw_out = "local_correlation_raw.tif"
    cog_out = "local_correlation_5x5_chunked_cog.tif"

    # Download files from MinIO
    with open(temp_dem, "wb") as f:
        f.write(client.get_object(client_id, dem_artifact_url).read())
    with open(temp_lst, "wb") as f:
        f.write(client.get_object(client_id, lst_artifact_url).read())

    dem_ds = gdal.Open(temp_dem, gdal.GA_ReadOnly)
    lst_ds = gdal.Open(temp_lst, gdal.GA_ReadOnly)

    dem_proj = dem_ds.GetProjection()
    dem_gt = dem_ds.GetGeoTransform()
    dem_nodata = dem_ds.GetRasterBand(1).GetNoDataValue()
    width, height = dem_ds.RasterXSize, dem_ds.RasterYSize

    # Step 1: Reproject LST to match DEM
    drv = gdal.GetDriverByName("GTiff")
    aligned = drv.Create(aligned_lst, width, height, 1, gdal.GDT_Float32)
    aligned.SetGeoTransform(dem_gt)
    aligned.SetProjection(dem_proj)
    aligned.GetRasterBand(1).SetNoDataValue(-9999.0)
    gdal.ReprojectImage(lst_ds, aligned, lst_ds.GetProjection(), dem_proj, gdal.GRA_Bilinear)
    aligned = None

    # Step 2: Initialize output
    out_ds = drv.Create(raw_out, width, height, 1, gdal.GDT_Float32)
    out_ds.SetGeoTransform(dem_gt)
    out_ds.SetProjection(dem_proj)
    out_band = out_ds.GetRasterBand(1)
    out_band.SetNoDataValue(-9999.0)
    out_band.Fill(-9999.0)
    out_ds = None

    # Step 3: Chunk-wise correlation
    dem_ds = gdal.Open(temp_dem)
    lst_ds = gdal.Open(aligned_lst)
    out_ds = gdal.Open(raw_out, gdal.GA_Update)

    for y in range(0, height, chunk_size):
        for x in range(0, width, chunk_size):
            xsize = min(chunk_size + window_size - 1, width - x)
            ysize = min(chunk_size + window_size - 1, height - y)
            dem_chunk = read_array(dem_ds, x, y, xsize, ysize, dem_nodata)
            lst_chunk = read_array(lst_ds, x, y, xsize, ysize, -9999.0)

            if dem_chunk.shape != lst_chunk.shape:
                continue

            corr_chunk = compute_corr_chunk(dem_chunk, lst_chunk, pad)
            corr_chunk[np.isnan(corr_chunk)] = -9999.0
            write_array(out_ds, corr_chunk.astype(np.float32), x + pad, y + pad)

    dem_ds, lst_ds, out_ds = None, None, None

    # Step 4: Convert to COG
    tiff_to_cogtiff(raw_out, cog_out)


    # Step 5: Save to local or MinIO
    if store_artifact:
        save_raster_artifact(config, client_id, cog_out, file_path, store_artifact)
        print(f"{file_path}")
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print("NDVI computed successfully.")

    # Step 6: Cleanup
    for f in [temp_dem, temp_lst, aligned_lst, raw_out, cog_out]:
        if os.path.exists(f):
            os.remove(f)