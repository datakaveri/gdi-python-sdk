import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")
import pickle as pkl
import os 

def download_features(config : str, client_id : str, artefact_url : str, save_as : str): 
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())
            data.to_file('temp.gpkg', driver='GPKG')        
    except Exception as e:
        print(e)
   
    try:
        client.fput_object( client_id,save_as,'temp.gpkg')
        os.remove('temp.gpkg')
        print("File saved successfully as : ", save_as)
    except Exception as e:
        raise e
    
# download_features(config = '../config.json', client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', artefact_url = 'c652e79c-0c0e-4c29-b1a2-205e1c1d0e6d.pkl', save_as = 'itermediate/features_1_temp.gpkg')

