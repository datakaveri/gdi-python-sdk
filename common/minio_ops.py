import json
import geopandas as gpd
from minio import Minio
import os 

# Load the minio credentials from the config file and connect to the minio server 
def connect_minio(config:str, client_id:str)  -> Minio: 
    '''
    Connect to minio server
    '''

    try:
        with open(config, 'r') as file:
            creds = json.load(file)
        
        access_key = creds['minio_access_key']
        secret_key = creds['minio_secret_key']
        minio_url = creds['minio_url']

        client = Minio(minio_url, access_key=access_key, secret_key=secret_key,secure=False)
        if client.bucket_exists(client_id):
            print("Current Bucket:", client_id)
        else:
            client.make_bucket(client_id)
            print("New Bucket Created:", client_id)
        return client
    except Exception as e:
        raise e






# Load the minio credentials from the config file and connect to the minio server and save the file 

def connect_store_minio(config:str, client_id:str, data:dict, file_name:str):
    '''
    config : str : path to the config file
    client_id : str : client id for the minio bucket
    data : dict : data to be stored in the minio bucket
    file_name : str : name of the file to be stored in the minio bucket 

    '''
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

    gdf = gpd.GeoDataFrame(data['features'])
    gdf.to_file(file_name, driver='GPKG')

    try:
        result = client.fput_object(
            client_id, file_name, file_name
        )
        os.remove(file_name)
        print(f"Object saved successfully to Minio at : {client_id}/{file_name} ")
    except Exception as e:
        raise Exception(f"Error while saving file: {e}")



# get list of all the objects / data present in the minio bucket
def get_ls(config:str, client_id:str):
    client = connect_minio(config, client_id)
    try:
        
        objects = client.list_objects(bucket_name=client_id, recursive=True)
        for obj in objects:
                print(obj.object_name)
        
    except Exception as e:
        raise e
    


