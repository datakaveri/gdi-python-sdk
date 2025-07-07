import geopandas as gpd
import fiona
import os
import io
import tempfile
from typing import Optional
from common.minio_ops import connect_minio, stream_to_minio

# Enable KML support in Fiona
fiona.supported_drivers['KML'] = 'rw'

# Mapping of file extensions to Fiona drivers
DRIVER_MAPPING = {
    '.geojson': 'GeoJSON',
    '.shp': 'ESRI Shapefile',
    '.gpkg': 'GPKG',
    '.kml': 'KML'
}


def stream_to_minio(client, bucket, local_path, remote_path):
    """
    Upload a local file to a MinIO bucket.

    Parameters:
    - client: MinIO client object
    - bucket: name of the bucket
    - local_path: path to local file
    - remote_path: object name/key in MinIO
    """
    client.fput_object(bucket, remote_path, local_path)


def convert_vector_format(
    config_path: str,
    client_id: str,
    input_path: str,
    input_store: str,
    output_path: str,
    output_store: str,
) -> None:
    """
    Convert a vector file from one supported format to another, optionally reading from
    or writing to a MinIO bucket.

    Parameters:
    - config_path: Path to MinIO config JSON
    - client_id: MinIO bucket name (required if using MinIO stores)
    - input_path: Local path or MinIO object name of the source file
    - input_store: 'local' or 'minio'; where to read the source from
    - output_path: Local path or MinIO object name for the converted file
    - output_store: 'local' or 'minio'; where to write the result
    """
    # Validate store parameters
    if input_store not in ('local', 'minio') or output_store not in ('local', 'minio'):
        raise ValueError("input_store and output_store must be either 'local' or 'minio'.")

    # Validate file extensions
    src_ext = os.path.splitext(input_path)[1].lower()
    dst_ext = os.path.splitext(output_path)[1].lower()

    if src_ext == dst_ext:
        print("Source and destination formats are identical; skipping conversion.")
        return

    if src_ext not in DRIVER_MAPPING or dst_ext not in DRIVER_MAPPING:
        raise ValueError(f"Unsupported format. Supported extensions: {list(DRIVER_MAPPING.keys())}")

    # Prepare MinIO client if needed
    client = None
    if 'minio' in (input_store, output_store):
        if not client_id:
            raise ValueError("client_id must be provided when using MinIO store.")
        client = connect_minio(config_path, client_id)

    # Read the source into a GeoDataFrame
    if input_store == 'local':
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file '{input_path}' does not exist.")
        gdf = gpd.read_file(input_path)

    else:
        resp = client.get_object(client_id, input_path)
        with io.BytesIO(resp.read()) as bio:
            gdf = gpd.read_file(bio)

    # Ensure CRS is EPSG:4326
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Determine output driver
    driver = DRIVER_MAPPING[dst_ext]

    if output_store == 'local':
        # Save directly to local filesystem
        gdf.to_file(output_path, driver=driver)
        print(f"Converted file saved locally at '{output_path}'.")

    else:
        # Write to a temporary file, then upload to MinIO
        with tempfile.NamedTemporaryFile(suffix=dst_ext, delete=False) as tmp:
            temp_path = tmp.name

        try:
            gdf.to_file(temp_path, driver=driver)
            stream_to_minio(client, client_id, temp_path, output_path)
            print(f"Converted file stored in MinIO at '{output_path}'.")
        finally:
            os.remove(temp_path)
