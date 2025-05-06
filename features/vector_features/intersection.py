import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature
import io



def make_intersection(config : str, client_id : str, left_feature : str, right_feature : str,  store_artifact : str, file_path : str = None)-> None:
    """
    Function to intersect two geodataframes and save the intersected data to minio or locally.In editor it will be renamed as create-intersection.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    left_feature : str (Reactflow will take it from the previous step)
    right_feature : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, left_feature) as response:
            data_1 = gpd.read_file(io.BytesIO(response.read()))
           
        with client.get_object(client_id, right_feature) as response:
            data_2 = gpd.read_file(io.BytesIO(response.read()))
                    
    except Exception as e:
        print(e)

    try:
        intersected_data = data_1.overlay(data_2, how='intersection')
    except Exception as e:
        raise e
    
    if store_artifact:
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=intersected_data, file_path=file_path, config_path=config)

    else:
        print("Data not saved. Set store_artifacts to minio/local to save the data to minio or locally.")
        print("Computed geometry successfully")
        # print(gdata)

    return 
# make_intersection('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'buffer_item/data_1.pkl', 'buffer_item/data_2.pkl', True, 'intersected_items/intersected_1.pkl')
