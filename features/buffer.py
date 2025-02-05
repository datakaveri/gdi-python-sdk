import geopandas as gpd
from common.minio_ops import connect_minio
from shapely.geometry import Point, Polygon, LineString
import warnings
warnings.filterwarnings("ignore")
import pickle as pkl
import os 
import uuid


def create_geometry(geo_obj):
    if isinstance(geo_obj, Point):
        return geo_obj
    elif isinstance(geo_obj, dict):
        geo_type = geo_obj['type'].lower()
        coords = geo_obj['coordinates']
        
        if geo_type == 'point' and len(coords) >= 2:
            return Point(coords[0], coords[1])
        elif geo_type == 'polygon':
            return Polygon(coords[0])
        elif geo_type == 'linestring':
            return LineString(coords)
    return None


def make_buffer(config : str, client_id : str, artefact_url : str, buffer_d : float, store_artefacts : bool = False, file_path : str = None):
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artefact_url) as response:
            data = pkl.loads(response.read())        
    except Exception as e:
        print(e)

    try:
        data['geometry'] = data['geometry'].apply(create_geometry)
        gdata = gpd.GeoDataFrame(data, geometry='geometry')
        buffer_d = float(buffer_d)
        gdata.buffer(buffer_d)
    except Exception as e:
        raise e
    
    if store_artefacts:
        if not file_path:
            file_path = f"{uuid.uuid4()}.pkl"
        try:
            gdata.to_pickle('temp.pkl')
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
        # print(gdata)
    


# data = make_buffer('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', '1b2a07b7-f423-4dd3-bdee-9a6af6fe47f9.pkl', 0.5, True, 'buffered_artefacts/bufferd_1.pkl')
# print(data)