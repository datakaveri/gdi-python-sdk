import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")
import pickle as pkl

def count_features(config : str, client_id : str, artefact_url : str, store_artefacts : bool = False):
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())
        
    except Exception as e:
        print(e)

    return str(data.count().iloc[3])

