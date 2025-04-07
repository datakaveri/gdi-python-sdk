import io
import numpy as np
import rasterio
import geopandas as gpd
from skimage.measure import find_contours
from shapely.geometry import LineString
from tqdm import tqdm
import warnings
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

warnings.filterwarnings("ignore")

def isometric_lines(
    config: str,
    client_id: str,
    artifact_url: str,
    interval: int = 10,
    store_artifact: bool = False,
    file_path: str = None
) -> str:
    """
    Generate isometric (contour) lines from DEM read from MinIO and given interval.
    Optionally upload the result GeoJSON to MinIO.

    Parameters
    ------------
    config : str (React flow will translate it as input)
    client_id : str (React flow will translate it as input)
    artifact_url : str (React flow will take it from the previous step)
    interval : int (React flow will translate it as input)
    store_artifact : enum [True, False] (React flow will translate it as input)
    file_path : str (React flow will ignore this parameter)
    """

    minio_client = connect_minio(config, client_id)

    try:
        with minio_client.get_object(client_id, artifact_url) as response:
            dem_bytes = response.read()
        print(f"[INFO] Downloaded DEM from MinIO: {artifact_url}")
    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed to download DEM from MinIO: {e}")

    try:
        # Step 1: Read raster data using rasterio from bytes
        with io.BytesIO(dem_bytes) as dem_buffer:
            with rasterio.open(dem_buffer) as src:
                if src.count != 1:
                    raise ValueError("Input raster must have only one band.")
                data = src.read(1)
                transform = src.transform
                raster_crs = src.crs

        # Step 2: Define contour levels
        min_val, max_val = np.nanmin(data), np.nanmax(data)
        levels = np.arange(min_val, max_val, interval)

        if len(levels) < 1:
            raise ValueError("Contour interval is too large. Decrease the interval to generate contours.")
        elif len(levels) > 1000:
            raise ValueError("Contour interval is too small. Increase the interval to limit contour generation.")

        # Step 3: Extract contour lines
        geometries = []
        contour_values = []

        for level in tqdm(levels, desc="Generating contours"):
            contours = find_contours(data, level)
            for contour in contours:
                coords = [tuple(transform * (x, y)) for y, x in contour]
                if len(coords) > 1:
                    geometries.append(LineString(coords))
                    contour_values.append(level)

        # Step 4: Create GeoDataFrame
        geo_df = gpd.GeoDataFrame({'level': contour_values, 'geometry': geometries}, crs=raster_crs)

        if store_artifact:
            save_feature(client_id=client_id, store_artifact=store_artifact, gdf=geo_df, file_path=file_path, config_path=config)

        else:
            print("Data not saved. Set store_artifact to 'minio' or 'local' to save the data.")
            print("Clipping completed successfully.")

        return 

    except Exception as e:
        raise RuntimeError(f"[ERROR] Contour generation failed: {e}")


# isometric_lines(
#     config="config.json",
#     client_id="c669d152-592d-4a1f-bc98-b5b73111368e",
#     artifact_url="downloaded_from_stac/Digital Elevation Model (DEM) at 50 K, Varanasi/VARANASI_DEM_50K - TIF_cog.tif",
#     interval=2,
#     store_artifact=True,
#     file_path="contour/contours_2m.geojson"
# )
