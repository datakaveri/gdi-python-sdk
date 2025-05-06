import requests
from pystac_client import Client, ItemSearch
import warnings
from .search_cat import get_stac_collection, get_stac_item
from auth.stac_token_gen import StacTokenGenerator
warnings.filterwarnings("ignore")
import os
import io
from tqdm import tqdm
from common.minio_ops import connect_minio,stream_to_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact


def get_assets(client_id: str, client_secret: str, role: str, collection_ids: str, config: str, store_artifact: str = 'minio', dir_path: str = None, item_id: str = None) -> None:
    """
    Function to download STAC assets. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as get-stac-assets.
    Parameters
    ----------
    client_id : str (Reactflow will translate it as input)
    client_secret : str (Reactflow will translate it as input)
    role : str (Reactflow will translate it as input)
    collection_ids : str (Reactflow will translate it as input)
    config : str (Reactflow will ignore this parameter)
    store_artifact : str (Reactflow will ignore this parameter)
    dir_path : str (Reactflow will ignore this parameter)  
    item_id: str (Reactflow will translate it as input)
    """
    links_list = []

    if item_id is not None:
        links_dict = get_stac_item([collection_ids], item_id)
    else:
        links_dict = get_stac_collection([collection_ids])
    token_generator = StacTokenGenerator(client_id, client_secret, role, collection_ids)
    auth_token = token_generator.generate_token()
    headers = {"Authorization": f"Bearer {auth_token}"}
    client = connect_minio(config, client_id)

    try:
        for folder_name, assets in links_dict.items():
            # for title, url in tqdm(assets.items(), desc=f"Downloading assets for {folder_name}"):
            for title, url in assets.items():
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                
                # Extract only the band part from the title
                band_name = title.split(" - ")[-1]  # Adjust this split logic if needed
                if dir_path is None:
                    dir_path = "downloaded_from_stac"
                # Construct filename inside MinIO bucket as key/{band_name}.tif
                filename = f"{dir_path}/{folder_name}/{folder_name}_{band_name}_cog.tif"
                links_list.append(filename) # use join https://www.w3schools.com/python/ref_string_join.asp
                # file_data = io.BytesIO(response.content)
                
                # Write temp files of tiff and geotiff locally
                temp_tif = "temp_geotiff.tif"
                with open(temp_tif, "wb") as f:
                    f.write(response.content)

                # Convert to COG
                temp_cogtif = "temp_cogtiff.tif"
                tiff_to_cogtiff(temp_tif, temp_cogtif)

                # Save via save_raster_artifact
                save_raster_artifact(
                    config=config,
                    client_id=client_id,
                    local_path=temp_cogtif,
                    file_path=filename,
                    store_artifact=store_artifact
                )

                #remove temp files
                os.remove(temp_tif)
                os.remove(temp_cogtif)
                

    except Exception as e:
        print(f"Failed to download assets:\n {e}")

    output_paths = '$'.join(links_list)
    print(output_paths)

  
# Example Usage
# client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb'
# client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# role = "consumer"
# get_assets(client_id, client_secret, role, '28e16f74-0ff8-4f11-a509-60fe078d8d47', '../../config.json')
