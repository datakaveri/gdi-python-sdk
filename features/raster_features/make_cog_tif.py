import boto3
import os
from botocore.client import Config
import json
from osgeo import gdal

def connect_boto3(config:str, client_id:str) -> None:
    with open(config, 'r') as file:
            creds = json.load(file)

    MINIO_ENDPOINT = creds['minio_url']
    if "http://" not in MINIO_ENDPOINT:
        MINIO_ENDPOINT = "http://" + MINIO_ENDPOINT
    ACCESS_KEY =  creds['minio_access_key']
    SECRET_KEY =  creds['minio_secret_key']
    BUCKET_NAME = client_id
    return MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET_NAME


def save_cog(config:str, client_id:str, prefix:str):

    MINIO_ENDPOINT, ACCESS_KEY, SECRET_KEY, BUCKET_NAME = connect_boto3(config, client_id)


    # Initialize MinIO S3 Client
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    # List all objects in 'download_from_stac/' folder
    objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if "Contents" in objects:
        for obj in objects["Contents"]:
            file_key = obj["Key"]  # Full path in MinIO

            if file_key.endswith(".tif"):  # Process only TIFF files
                # Extract subfolder and filename
                relative_path = file_key.replace(f"{prefix}/", "")  # Remove base path
                subfolder_path = os.path.dirname(relative_path)  # e.g., C3_MX_20240421_2484919101
                filename = os.path.basename(relative_path)  # e.g., BAND1.tif

                # Local temp file paths
                local_tif = f"{filename}"
                local_cog = f"{filename.replace('.tif', '_cog.tif')}"

                # Download the TIFF file
                s3_client.download_file(BUCKET_NAME, file_key, local_tif)
                 
                # Convert to COG using GDAL               
                translate_options = gdal.TranslateOptions(format="COG", metadataOptions=["COPY_SRC_OVERVIEWS=YES"])
                gdal.Translate(local_cog, local_tif, options=translate_options)

                # Define output key with the same subfolder structure in "cogtiffs/"
                output_key = f"cogtiffs_from_stac/{subfolder_path}/{filename.replace('.tif', '_cog.tif')}"

                # Upload the COG to MinIO
                s3_client.upload_file(local_cog, BUCKET_NAME, output_key)

                print(f"Converted {file_key} -> {output_key}")

                # Cleanup
                os.remove(local_tif)
                os.remove(local_cog)

    else:
        print("No TIFF files found in 'download_from_stac/'.")



# save_cog('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'downloaded_from_stac')