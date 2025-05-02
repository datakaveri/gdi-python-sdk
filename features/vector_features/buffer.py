import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature
import io
import warnings
warnings.filterwarnings("ignore")


def make_buffer(config : str, client_id : str, artifact_url : str, buffer_d : float, store_artifact : str, file_path : str = None) -> None:
    """
    Function to buffer the geometries in a geodataframe and save the buffered data to minio or locally.In editor it will be renamed as create-buffer.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    buffer_d : float (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try :
        with client.get_object(client_id, artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))       
    except Exception as e:
        print(e)

    try:        
        buffer_d = float(buffer_d)
        gdf = gdf.to_crs(epsg=7755)
        gdf['geometry'] = gdf['geometry'].buffer(buffer_d)
    except Exception as e:
        raise e
    
    if store_artifact:
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=gdf, file_path=file_path, config_path=config)

    else:
        print("Data not saved. Set store_artifacts to minio/local to save the data to minio or locally.")
        print("Computed geometry successfully")
        # print(gdata)
    


# data = make_buffer('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', '1b2a07b7-f423-4dd3-bdee-9a6af6fe47f9.pkl', 0.5, minio, 'buffered_artifacts/bufferd_1.pkl')
# print(data)
