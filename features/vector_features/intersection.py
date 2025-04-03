import geopandas as gpd

from common.minio_ops import connect_minio
import pickle as pkl
import os 
import uuid


def make_intersection(config : str, client_id : str, left_feature : str, right_feature : str,  store_artefacts : bool = False, file_path : str = None)-> None:
    """
    Function to intersect two geodataframes and save the intersected data to minio.In editor it will be renamed as create-intersection.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    left_feature : str (Reactflow will take it from the previous step)
    right_feature : str (Reactflow will take it from the previous step)
    store_artefacts : enum [True, False] (Reactflow will translate it as input)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, left_feature) as response:
            data_1 = pkl.loads(response.read())
           
        with client.get_object(client_id, right_feature) as response:
            data_2 = pkl.loads(response.read())
                    
    except Exception as e:
        print(e)

    try:
        intersected_data = data_1.overlay(data_2, how='intersection')
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
            print(file_path)
            # return gdata
        except Exception as e:
            raise Exception(f"Error while saving file: {e}")
    else:
        print("Data not saved. Set store_artefacts to True to save the data to minio.")
        print("Data buffered successfully")



# make_intersection('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'buffer_item/data_1.pkl', 'buffer_item/data_2.pkl', True, 'intersected_items/intersected_1.pkl')
