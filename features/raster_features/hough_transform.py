import os
import warnings
import numpy as np
import cv2
from osgeo import gdal
from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff_v2
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")


def get_hough_transform(config: str, client_id: str, artifact_url: str,
                             store_artifact: str, file_path: str = None,
                             method: str = "line", **kwargs) -> None:
    """
    Function to perform Hough Transform (lines or circles) on each raster band using GDAL and OpenCV. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as generate-hough-transform.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    method : str (Reactflow will translate it as input, This parameter will be optional)
    **kwargs : dict (Reactflow will translate it as input, This parameter will be optional)
    """

    # --- Step 1: Connect to MinIO and fetch raster ---
    client = connect_minio(config, client_id)

    temp_input = "temp_input.tif"
    temp_hough_raw = "temp_hough_raw.tif"
    temp_hough_cog = "temp_hough_cog.tif"

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

    output_images = []

    # --- Step 3: Validate method and arguments ---
    valid_line_args = {'canny_thresh1', 'canny_thresh2', 'hough_thresh', 'min_line_length', 'max_line_gap'}
    valid_circle_args = {'dp', 'min_dist', 'param1', 'param2', 'min_radius', 'max_radius'}
    input_args = set(kwargs.keys())

    if method == 'line':
        if len(input_args & valid_circle_args) > 0:
            raise ValueError("Circle parameters provided for method='line'. Only line parameters allowed.")
    elif method == 'circle':
        if len(input_args & valid_line_args) > 0:
            raise ValueError("Line parameters provided for method='circle'. Only circle parameters allowed.")
    else:
        raise ValueError("method must be either 'line' or 'circle'.")

    # --- Step 4: Apply Hough Transform per band ---
    for b in range(1, bands + 1):
        band = dataset.GetRasterBand(b)
        data = band.ReadAsArray().astype(np.uint8)

        if method == 'line':
            canny_thresh1 = kwargs.get('canny_thresh1', 100)
            canny_thresh2 = kwargs.get('canny_thresh2', 200)
            hough_thresh = kwargs.get('hough_thresh', 50)
            min_line_length = kwargs.get('min_line_length', 10)
            max_line_gap = kwargs.get('max_line_gap', 10)

            edges = cv2.Canny(data, canny_thresh1, canny_thresh2)
            lines = cv2.HoughLinesP(edges, 1, np.pi / 180, hough_thresh,
                                    minLineLength=min_line_length, maxLineGap=max_line_gap)

            line_img = np.zeros_like(data, dtype=np.uint8)
            if lines is not None:
                for line in lines:
                    x1, y1, x2, y2 = line[0]
                    cv2.line(line_img, (x1, y1), (x2, y2), 255, 1)
            output_images.append(line_img)

        elif method == 'circle':
            dp = kwargs.get('dp', 1)
            min_dist = kwargs.get('min_dist', 20)
            param1 = kwargs.get('param1', 100)
            param2 = kwargs.get('param2', 30)
            min_radius = kwargs.get('min_radius', 0)
            max_radius = kwargs.get('max_radius', 0)

            img_blur = cv2.medianBlur(data, 5)
            circles = cv2.HoughCircles(img_blur, cv2.HOUGH_GRADIENT, dp, min_dist,
                                       param1=param1, param2=param2,
                                       minRadius=min_radius, maxRadius=max_radius)
            circle_img = np.zeros_like(data, dtype=np.uint8)
            if circles is not None:
                circles = np.uint16(np.around(circles))
                for c in circles[0, :]:
                    cv2.circle(circle_img, (c[0], c[1]), c[2], 255, 2)
            output_images.append(circle_img)

    # --- Step 5: Save output raster ---
    out_stack = np.stack(output_images, axis=0)
    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(temp_hough_raw, cols, rows, bands, gdal.GDT_Byte)
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)

    for i in range(bands):
        out_band = out_ds.GetRasterBand(i + 1)
        out_band.WriteArray(out_stack[i])
        out_band.FlushCache()

    out_ds = None
    dataset = None

    # --- Step 6: Convert to COG ---
    try:
        tiff_to_cogtiff_v2(temp_hough_raw, temp_hough_cog)
    except Exception as e:
        raise RuntimeError(f"[ERROR] Could not convert Hough TIF to COG: {e}")

    # --- Step 7: Save artifact ---
    if store_artifact:
        save_raster_artifact(
            config=config,
            client_id=client_id,
            local_path=temp_hough_cog,
            file_path=file_path,
            store_artifact=store_artifact
        )
        print(f"{file_path}")
    else:
        print("Data not saved. Set store_artifact to minio/local to save the data.")
        print(f"Hough Transform ({method}) completed successfully.")

    # --- Step 8: Cleanup temporary files ---
    try:
        for fpath in [temp_input, temp_hough_raw, temp_hough_cog]:
            if os.path.exists(fpath):
                os.remove(fpath)
    except Exception as e:
        print(f"[WARN] Failed to clean up intermediate files: {e}")
