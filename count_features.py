import json
import geopandas as gpd
from minio import Minio
import warnings
warnings.filterwarnings("ignore")

def count_features(config : str, client_id : str, artefact_url : str, store_artefacts : bool = False):
    
    try:
        with open(config, 'r') as file:
            creds = json.load(file)
        
        access_key = creds['minio_access_key']
        secret_key = creds['minio_secret_key']
        minio_url = creds['minio_url']

        client = Minio(minio_url, access_key=access_key, secret_key=secret_key,secure=False)
    except Exception as e:
        raise e

    if client.bucket_exists(client_id):
        print("Current Bucket:", client_id)
    else:
        client.make_bucket(client_id)
        print("New Bucket Created:", client_id)

    try :
        data = client.get_object(client_id, artefact_url)
        gdf = gpd.read_file(data)
    except Exception as e:
        print(e)

    return "Feature count = " + str(gdf.count().iloc[3])

# count = count_features('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'data_3.gpkg', True)
# print(count)