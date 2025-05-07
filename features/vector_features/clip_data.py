import io
import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

def make_clip(
    config: str,
    client_id: str,
    target_artifact_url: str,
    clip_artifact_url: str,
    store_artifact: str,
    file_path: str = None
) -> dict:
    """
    Clip a target GeoDataFrame with another GeoDataFrame (clip layer). Optionally upload the clipped result back to MinIO or save locally.In editor it will be renamed as clip-vector.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    target_artifact_url : str (Reactflow will take it from the previous step)
    clip_artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try:
        with client.get_object(client_id, target_artifact_url) as target_response:
            target_gdf = gpd.read_file(io.BytesIO(target_response.read()))

        with client.get_object(client_id, clip_artifact_url) as clip_response:
            clip_gdf = gpd.read_file(io.BytesIO(clip_response.read()))
        
        if not isinstance(target_gdf, gpd.GeoDataFrame) or not isinstance(clip_gdf, gpd.GeoDataFrame):
            raise TypeError("Both target and clip data must be GeoDataFrames.")
        
        if target_gdf.crs != clip_gdf.crs:
            raise ValueError("CRS mismatch: Target and Clip GeoDataFrames must have the same CRS.")

        clipped_data = gpd.clip(target_gdf, clip_gdf)

        if store_artifact:
            save_feature(client_id=client_id, store_artifact=store_artifact, gdf=clipped_data, file_path=file_path, config_path=config)

        else:
            print("Data not saved. Set store_artifact to 'minio' or 'local' to save the data.")
            print("Clipping completed successfully.")

        return 

    except Exception as e:
        raise RuntimeError(f"Error while performing clip operation: {e}")
    

# make_clip(
#   config ='config.json',
#   client_id='c669d152-592d-4a1f-bc98-b5b73111368e',
#   target_artifact_url='Voronoi/Voronoi_var_scl.geojson',
#   clip_artifact_url= 'vectors/Census_Abstract_Varanasi_ce17126b-4972-4971-af44-c8c9de57a98a.geojson',
#   store_artifact='minio',
#   file_path='vec_clip/clipped_data.geojson')

