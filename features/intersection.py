import geopandas as gpd
from common.minio_ops import connect_minio
import pickle as pkl
import os 
import uuid


def make_intersection(config : str, client_id : str, artefact_url_1 : str, artefact_url_2 : str,  store_artefacts : bool = False, file_path : str = None):
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url_1) as response:
            data_1 = pkl.loads(response.read())
        with client.get_object(client_id, artefact_url_2) as response:
            data_2 = pkl.loads(response.read())        
    except Exception as e:
        print(e)

    try:
        intersected_data = data_1.intersection(data_2, align=True)
        intersected_data.to_pickle('temp.pkl')
    except Exception as e:
        raise e
    
    if store_artefacts:
        if not file_path:
            file_path = f"{uuid.uuid4()}.pkl"
        try:
           
            client.fput_object(
                client_id, file_path, 'temp.pkl'
            )
            os.remove('temp.pkl')
            print("Intersected data saved successfully at: ", file_path)
            # return gdata
        except Exception as e:
            raise Exception(f"Error while saving file: {e}")
    else:
        print("Data not saved. Set store_artefacts to True to save the data to minio.")
        print("Data buffered successfully")



# make_intersection('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'buffer_item/data_1.pkl', 'buffer_item/data_2.pkl', True, 'intersected_items/intersected_1.pkl')