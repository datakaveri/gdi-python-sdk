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
    config : str (Node red will translate it as input)
    client_id : str (Node red will translate it as input)
    artefact_url : str (Node red will take it from the previous step)
    save_as : str (Node red will ignore this)

    """ 
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())
            data.to_file('temp.geojson', driver='GeoJSON')        
    except Exception as e:
        print(e)
   
    try:
        client.fput_object( client_id,save_as,'temp.geojson')
        pre_signed_url = client.get_presigned_url("GET",client_id, save_as, expires=timedelta(days=1))
        os.remove('temp.geojson')
        print(pre_signed_url)
    except Exception as e:
        raise e
    
# download_features(config = '../../config.json', client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', artefact_url = 'intermediate/paginated_data.pkl', save_as = 'intermediate/features_1_temp.geojson')

