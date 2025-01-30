import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")

def count_features(config : str, client_id : str, artefact_url : str, store_artefacts : bool = False):
    
    client = connect_minio(config, client_id)

    try :
        data = client.get_object(client_id, artefact_url)
        gdf = gpd.read_file(data)
    except Exception as e:
        print(e)

    return str(gdf.count().iloc[3])

