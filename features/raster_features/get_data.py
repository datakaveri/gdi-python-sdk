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

def get_assets(client_id: str, client_secret: str, role: str, collection_ids: str, config: str) -> None:
    '''Download Cartosat images from the STAC browser and stream to minio
    
    Parameters:
    ---------------
    client_id: str (client_id, same as the bucket name)
    client_secret:str (client_secret for authentication)
    role: str (role for the token)
    collection_ids: str (collection_ids for the STAC browser)
    config: str (path to the minio config file)
    '''
    links_dict = search_get_stac([collection_ids])
    token_generator = StacTokenGenerator(client_id, client_secret, role, collection_ids)
    auth_token = token_generator.generate_token()
    headers = {"Authorization": f"Bearer {auth_token}"}
    client = connect_minio(config, client_id)

    try:
        for folder_name, urls in links_dict.items(): 
            for url in tqdm(urls, desc=f"Downloading assets for {folder_name}"):
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                # Construct filename inside "downloaded_assets/<folder_name>/"
                filename = f"downloaded_assets/{folder_name}/{url.split('/')[-1]}.tif"
                file_data = io.BytesIO(response.content)
                stream_to_minio(client, client_id, filename, file_data, len(response.content))

    except Exception as e:
        print(f"Failed to download assets:\n {e}")

# Example Usage
# client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb'
# client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# role = "consumer"
# get_assets(client_id, client_secret, role, '28e16f74-0ff8-4f11-a509-60fe078d8d47', '../../config.json')
