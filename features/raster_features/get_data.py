import requests
from pystac_client import Client, ItemSearch
import warnings
from .search_cat import search_get_stac
from auth.stac_token_gen import StacTokenGenerator
warnings.filterwarnings("ignore")
import os
import io
from tqdm import tqdm
from common.minio_ops import connect_minio,stream_to_minio
from common.convert_to_cog import tiff_to_cogtiff


def get_assets(client_id: str, client_secret: str, role: str, collection_ids: str, config: str) -> None:
    '''
    Download Cartosat images from the STAC browser and stream to MinIO with band-based naming.
    
    Parameters
    ---------------
    client_id: str (client_id, same as the bucket name)
    client_secret: str (client_secret for authentication)
    role: str (role for the token)
    collection_ids: str (collection_ids for the STAC browser)
    config: str (path to the MinIO config file)
    '''
    links_dict = search_get_stac([collection_ids])
    token_generator = StacTokenGenerator(client_id, client_secret, role, collection_ids)
    auth_token = token_generator.generate_token()
    headers = {"Authorization": f"Bearer {auth_token}"}
    client = connect_minio(config, client_id)

    try:
        for folder_name, assets in links_dict.items():
            for title, url in tqdm(assets.items(), desc=f"Downloading assets for {folder_name}"):
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                # Extract only the band part from the title
                band_name = title.split(" - ")[-1]  # Adjust this split logic if needed

                # Construct filename inside MinIO bucket as key/{band_name}.tif
                filename = f"downloaded_from_stac/{folder_name}/{band_name}_cog.tif"
                
                # file_data = io.BytesIO(response.content)
                
                # Write temp files of tiff and geotiff locally
                temp_tif = "temp_geotiff.tif"
                temp_cogtif = "temp_cogtiff.tif"
                with open(temp_tif, "wb") as f:
                    f.write(response.content)

                # Convert the tiff files    
                cog_tif = tiff_to_cogtiff(temp_tif, temp_cogtif)
    
                #upload to minio 
                stream_to_minio(client, client_id, filename, cog_tif)

                #remove temp files
                os.remove(temp_tif)
                os.remove(temp_cogtif)

    except Exception as e:
        print(f"Failed to download assets:\n {e}")


# Example Usage
# client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb'
# client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# role = "consumer"
# get_assets(client_id, client_secret, role, '28e16f74-0ff8-4f11-a509-60fe078d8d47', '../../config.json')
