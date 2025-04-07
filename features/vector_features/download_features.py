import geopandas as gpd
from common.minio_ops import connect_minio
import warnings
warnings.filterwarnings("ignore")
import os 
import io
from datetime import timedelta
import uuid
def download_features(config : str, client_id : str, artefact_url : str, save_as : str) -> str:
    """
    Download features from the minio bucket and save it as a geopackage file.In editor it will be renamed as download-artifact.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artefact_url : str (Reactflow will take it from the previous step)
    save_as : str (Reactflow will ignore this parameter)
    """ 
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = gpd.read_file(io.BytesIO(response.read()))  
            data.to_file('temp.geojson', driver='GeoJSON')        
    except Exception as e:
        print(e)
   
    try:
        if save_as is None:
            save_as = f'downloadable/{str(uuid.uuid4())}.geojson'

        client.fput_object( client_id,save_as,'temp.geojson')
        pre_signed_url = client.get_presigned_url("GET",client_id, save_as, expires=timedelta(days=1))
        os.remove('temp.geojson')
        print(pre_signed_url)
    except Exception as e:
        raise e
    
# download_features(config = '../../config.json', client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', artefact_url = 'intermediate/paginated_data.pkl', save_as = 'intermediate/features_1_temp.geojson')

