import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")
import pickle as pkl

def count_features(config : str, client_id : str, artefact_url : str, store_artefacts : bool = False):
    """Function to count the number of features in a geodataframe and save the count to minio.
     Parameters:
    ------------:
    config : str : path to the config file
    client_id : str : client id of the user
    artefact_url : str : url of the artefact to be counted
    store_artefacts : bool : whether to store the count in minio
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())
        
    except Exception as e:
        print(e)

    return str(data.count().iloc[3])

