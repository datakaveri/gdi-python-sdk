import os
import io
import uuid
import numpy as np
import rioxarray
import warnings
from common.minio_ops import connect_minio, stream_to_minio
from features.raster_features.convert_to_cog import tiff_to_cogtiff

warnings.filterwarnings("ignore")


def flood_fill(
    config: str,
    client_id: str,
    artifact_url: str,
    threshold: float,
    store_artifact: bool = False,
    file_path: str = None
) -> str:
    """
    Generate flood inundated raster based on DEM read from MinIO and input threshold value.
    Optionally upload the clipped result back to MinIO.
    
    Parameters
    ------------
    config : str (React flow will translate it as input)
    client_id : str (React flow will translate it as input)
    artifact_url : str (React flow will take it from the previous step)
    threshold : float (React flow will translate it as input)
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
        # Load DEM into rioxarray from bytes
        with io.BytesIO(dem_bytes) as dem_buffer:
            da_dem = rioxarray.open_rasterio(dem_buffer).isel(band=0)
            da_dem = da_dem.where(da_dem > 0, np.nan)

        min_elev = da_dem.min().item()
        max_elev = da_dem.max().item()
        valid_range = (min_elev - 1, max_elev + 1)
        print(f"Min Elev: {min_elev}, Max Elev: {max_elev}, Valid threshold: {valid_range}")

        if not (valid_range[0] <= threshold <= valid_range[1]):
            raise ValueError(f"Threshold {threshold} is out of range.")

        # Flood fill logic
        binary_mask = np.where(da_dem.values < threshold, 1, 0)
        sy, sx = np.unravel_index(da_dem.argmin(), da_dem.shape)
        print("Starting flood fill...")

        filled = set()
        fill = set([(sx, sy)])
        height, width = binary_mask.shape
        flood = np.zeros_like(binary_mask, dtype=np.int8)

        while fill:
            x, y = fill.pop()
            if x < 0 or x >= width or y < 0 or y >= height:
                continue
            if binary_mask[y, x] == 1:
                flood[y, x] = 1
                filled.add((x, y))
                for neighbor in [(x-1, y), (x+1, y), (x, y-1), (x, y+1)]:
                    if neighbor not in filled:
                        fill.add(neighbor)

        print("Flood fill completed.")

        # Create flood output
        flooded_da = da_dem.copy()
        flooded_da.values = flood

        if store_artifact:
            if not file_path:
                file_path = f"flood_outputs/flood_{uuid.uuid4()}.tif"

            # Write GeoTIFF locally
            temp_tif = "temp_geotiff.tif"
            temp_cogtif = "temp_cogtiff.tif"
            flooded_da.rio.to_raster(temp_tif)

            # Convert to COG
            cog_path = tiff_to_cogtiff(temp_tif, temp_cogtif)

            # Upload to MinIO using stream_to_minio
            stream_to_minio(minio_client, client_id, file_path, cog_path)
            print(f"[INFO] COG Flood raster saved to MinIO: {file_path}")

            # Cleanup
            os.remove(temp_tif)
            os.remove(temp_cogtif)

            return file_path

        else:
            print("[INFO] store_artifact=False. Output not saved to MinIO.")
            return "Flood fill done, output not saved."

    except Exception as e:
        raise RuntimeError(f"[ERROR] Flood inundation processing failed: {e}")


# flood_fill(
#         config="config.json",
#         client_id="c669d152-592d-4a1f-bc98-b5b73111368e",
#         artifact_url="downloaded_from_stac/Digital Elevation Model (DEM) at 50 K, Varanasi/VARANASI_DEM_50K - TIF_cog.tif",
#         threshold=80,
#         store_artifact=True,
#         file_path="flood/flood_inundation_varanasi.tif"
#     )