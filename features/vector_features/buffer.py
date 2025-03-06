import geopandas as gpd
from common.minio_ops import connect_minio

import warnings
warnings.filterwarnings("ignore")
import pickle as pkl
import os 
import uuid



def make_buffer(config : str, client_id : str, artefact_url : str, buffer_d : float, store_artefacts : bool = False, file_path : str = None) -> None:
    """
    Function to buffer the geometries in a geodataframe and save the buffered data to minio.
    Parameters
    ----------
    config : str (Node red will translate it as input)
    client_id : str (Node red will translate it as input)
    artefact_url : str (Node red will translate it as input)
    buffer_d : float (Node red will translate it as input)
    store_artefacts : enum [True, False] (Node red will translate it as input)
    file_path : str (Node red will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())        
    except Exception as e:
        print(e)

    try:        
        buffer_d = float(buffer_d)
        data['geometry'] = data['geometry'].buffer(buffer_d)
    except Exception as e:
        raise e
    
    if store_artefacts:
        if not file_path:
            # file_path = f"{uuid.uuid4()}.pkl"
            file_path = f"{uuid.uuid4()}.pkl"
        
        data.to_pickle('hello.pkl')
          
        try:
            print("Saving file to minio as ", file_path)
            client.fput_object(client_id, file_path, 'hello.pkl')
        except Exception as e:
            raise Exception(f"Error while saving file: {e}")
        
        os.remove('hello.pkl')
        print(file_path)
            # return gdata
        
    else:
        print("Data not saved. Set store_artefacts to True to save the data to minio.")
        print("Data buffered successfully")
        # print(gdata)
    


# data = make_buffer('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', '1b2a07b7-f423-4dd3-bdee-9a6af6fe47f9.pkl', 0.5, True, 'buffered_artefacts/bufferd_1.pkl')
# print(data)
