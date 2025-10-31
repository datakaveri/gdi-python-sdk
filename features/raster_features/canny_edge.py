import os
import warnings
import numpy as np
import cv2
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff_v2
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")


def compute_canny_edge(config: str, client_id: str, artifact_url: str,
                       store_artifact: str, file_path: str = None,
                       threshold1: int = 100, threshold2: int = 200) -> None:
    """
    Function to perform Canny edge detection on each raster band using GDAL and OpenCV. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate_canny_edge.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    threshold1 : float (Reactflow will translate it as input)
    threshold2 : float (Reactflow will translate it as input)
    """

    client = connect_minio(config, client_id)

    temp_input = "temp_input.tif"
    temp_edge_raw = "temp_edge_raw.tif"
    temp_edge_cog = "temp_edge_cog.tif"

    # --- Step 1: Download raster from MinIO ---
    with client.get_object(client_id, artifact_url) as response:
        raster_data = response.read()
    with open(temp_input, "wb") as f:
        f.write(raster_data)

    # --- Step 2: Open raster with GDAL ---
    dataset = gdal.Open(temp_input, gdal.GA_ReadOnly)
    if dataset is None:
        raise FileNotFoundError(f"Could not open raster: {temp_input}")

    bands = dataset.RasterCount
    cols = dataset.RasterXSize
    rows = dataset.RasterYSize
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()

    edge_images = []

    # --- Step 3: Apply Canny edge detection per band ---
    for b in range(1, bands + 1):
        band = dataset.GetRasterBand(b)
        data = band.ReadAsArray()

        # Normalize to 8-bit range
        data = data.astype(np.uint8)

        # Apply Canny Edge Detection
        edges = cv2.Canny(data, threshold1=threshold1, threshold2=threshold2)
        edge_images.append(edges)

    edge_stack = np.stack(edge_images, axis=0)

    # --- Step 4: Create output GeoTIFF ---
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(temp_edge_raw, cols, rows, bands, gdal.GDT_Byte)
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)

    for i in range(bands):
        out_band = out_ds.GetRasterBand(i + 1)
        out_band.WriteArray(edge_stack[i])
        out_band.FlushCache()

    out_ds = None
    dataset = None

    # --- Step 5: Convert to COG ---
    try:
        tiff_to_cogtiff_v2(temp_edge_raw, temp_edge_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert edge TIF to COG: {e}")

    # --- Step 6: Save to MinIO or local ---
    if store_artifact:
        save_raster_artifact(
            config=config,
            client_id=client_id,
            local_path=temp_edge_cog,
            file_path=file_path,
            store_artifact=store_artifact
        )
        print(f"{file_path}")
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print("Canny edge detection completed successfully.")

    # --- Step 7: Cleanup temporary files ---
    try:
        for fpath in [temp_input, temp_edge_raw, temp_edge_cog]:
            if os.path.exists(fpath):
                os.remove(fpath)
    except Exception as e:
        print(f"[WARN] Failed to clean up intermediate files: {e}")
