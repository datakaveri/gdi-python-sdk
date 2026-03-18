import os
from osgeo import gdal
from common.minio_ops import connect_minio, stream_to_minio, get_bucket_name


def save_cog(config: str, prefix: str):
    bucket_name = get_bucket_name(config)
    client = connect_minio(config)

    # List all objects in the given prefix folder
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

    found_tif = False
    for obj in objects:
        file_key = obj.object_name  # Full path in MinIO

        if file_key.endswith(".tif"):  # Process only TIFF files
            found_tif = True
            # Extract subfolder and filename
            relative_path = file_key.replace(f"{prefix}/", "")  # Remove base path
            subfolder_path = os.path.dirname(
                relative_path
            )  # e.g., C3_MX_20240421_2484919101
            filename = os.path.basename(relative_path)  # e.g., BAND1.tif

            # Local temp file paths
            local_tif = f"{filename}"
            local_cog = f"{filename.replace('.tif', '_cog.tif')}"

            # Download the TIFF file
            client.fget_object(bucket_name, file_key, local_tif)

            # Convert to COG using GDAL
            translate_options = gdal.TranslateOptions(
                format="COG", metadataOptions=["COPY_SRC_OVERVIEWS=YES"]
            )
            gdal.Translate(local_cog, local_tif, options=translate_options)

            # Define output key with the same subfolder structure in "cogtiffs/"
            output_key = f"cogtiffs_from_stac/{subfolder_path}/{filename.replace('.tif', '_cog.tif')}"

            # Upload the COG to MinIO
            stream_to_minio(client, bucket_name, output_key, local_cog)

            print(f"Converted {file_key} -> {output_key}")

            # Cleanup
            os.remove(local_tif)
            os.remove(local_cog)

    if not found_tif:
        print("No TIFF files found in 'download_from_stac/'.")


# save_cog('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'downloaded_from_stac')
