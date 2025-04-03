import io
import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")

def count_features(config : str, client_id : str, artefact_url : str):
    """
    Function to count the number of features in a geodataframe.In editor it will be renamed as features-count.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    artefact_url : str (Reactflow will take it from the previous step)
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = gpd.read_file(io.BytesIO(response.read()))   
        
    except Exception as e:
        print(e)

    return str(data.count().iloc[3])

