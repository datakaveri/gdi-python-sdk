import os
import tempfile
from osgeo import gdal
from common.minio_ops import connect_minio, stream_to_minio

# Mapping of supported raster extensions to GDAL drivers
RASTER_DRIVER_MAPPING = {
    '.tif': 'GTiff',
    '.tiff': 'GTiff',
    '.img': 'HFA'
}

def stream_to_minio(client, bucket, local_path, remote_path):
    """
    Upload a local file to a MinIO bucket.
    """
    client.fput_object(bucket, remote_path, local_path)

def convert_raster_format(
    config_path: str,
    client_id: str,
    input_path: str,
    input_store: str,
    output_path: str,
    output_store: str
) -> None:
    """
    Function to convert a raster file from one supported format to another and save the buffered data to minio or locally.In editor it will be renamed as convert-raster.

    Parameters:
    - config_path: Path to MinIO config JSON
    - client_id: MinIO bucket name (required if using MinIO stores)
    - input_path: Local path or MinIO object name of the source file
    - input_store: 'local' or 'minio'
    - output_path: Local path or MinIO object name for the converted file
    - output_store: 'local' or 'minio'
    """

    if input_store not in ('local', 'minio') or output_store not in ('local', 'minio'):
        raise ValueError("input_store and output_store must be either 'local' or 'minio'.")

    # Get file extensions
    src_ext = os.path.splitext(input_path)[1].lower()
    dst_ext = os.path.splitext(output_path)[1].lower()

    supported_exts = list(RASTER_DRIVER_MAPPING.keys())

    if src_ext == '.ecw' or dst_ext == '.ecw':
        raise ValueError(
            f".ecw format is not supported in this raster conversion workflow. "
            f"Supported formats are: {supported_exts}"
        )

    if src_ext not in RASTER_DRIVER_MAPPING:
        raise ValueError(
            f"Unsupported input format: {src_ext}. Supported formats: {supported_exts}"
        )

    if dst_ext not in RASTER_DRIVER_MAPPING:
        raise ValueError(
            f"Unsupported output format: {dst_ext}. Supported formats: {supported_exts}"
        )

    if src_ext == dst_ext:
        print("Source and destination formats are identical; skipping conversion.")
        return

    # Prepare MinIO client if needed
    client = None
    if 'minio' in (input_store, output_store):
        if not client_id:
            raise ValueError("client_id must be provided when using MinIO store.")
        client = connect_minio(config_path, client_id)

    # Retrieve local input file path
    if input_store == 'local':
        local_input_path = input_path
        if not os.path.exists(local_input_path):
            raise FileNotFoundError(f"Input file '{local_input_path}' does not exist.")
    else:
        resp = client.get_object(client_id, input_path)
        with tempfile.NamedTemporaryFile(delete=False, suffix=src_ext) as tmp:
            tmp.write(resp.read())
            local_input_path = tmp.name

    driver_name = RASTER_DRIVER_MAPPING[dst_ext]

    # Create temp output file
    with tempfile.NamedTemporaryFile(delete=False, suffix=dst_ext) as tmp:
        local_output_path = tmp.name

    try:
        translate_options = gdal.TranslateOptions(format=driver_name)

        result = gdal.Translate(local_output_path, local_input_path, options=translate_options)

        if result is None:
            raise RuntimeError(f"Failed to convert raster to {driver_name}.")
        result = None

        if output_store == 'local':
            os.replace(local_output_path, output_path)
            print(f"Converted file saved locally at '{output_path}'.")
        else:
            stream_to_minio(client, client_id, local_output_path, output_path)
            print(f"Converted file stored in MinIO at '{output_path}'.")

    finally:
        if os.path.exists(local_output_path):
            os.remove(local_output_path)
        if input_store == 'minio' and os.path.exists(local_input_path):
            os.remove(local_input_path)