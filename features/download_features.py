import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")
import pickle as pkl
import os 
from datetime import timedelta
def download_features(config : str, client_id : str, artefact_url : str, save_as : str) -> str:
    """
    Download features from the minio bucket and save it as a geopackage file.

    Parameters:
    ------------
    config : input_str : path to the config file
    client_id : input_str : the client id of the user / bucket
    artefact_url : step_str : the url of the artefact to download
    save_as : input_str : the name of the file to save the features

    """ 
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())
            data.to_file('temp.gpkg', driver='GPKG')        
    except Exception as e:
        print(e)
   
    try:
        client.fput_object( client_id,save_as,'temp.gpkg')
        pre_signed_url = client.get_presigned_url("GET",client_id, save_as, expires=timedelta(days=1))
        os.remove('temp.gpkg')
        print(pre_signed_url)
    except Exception as e:
        raise e
    
# download_features(config = '../config.json', client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', artefact_url = 'intermediate/data_new1.pkl', save_as = 'intermediate/features_1_temp.gpkg')

