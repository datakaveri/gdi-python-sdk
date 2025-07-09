import io
import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

def bbox_clip_feature(
    config: str,
    client_id: str,
    target_artifact_url: str,
    clip_vector_path: str,  
    store_artifact: str,
    file_path: str = None
) -> dict:
    """
    Clip a target GeoDataFrame with a local GeoJSON (clip layer). Optionally upload the clipped result back to MinIO or save locally.In editor it will be renamed as bbox-vector-clip.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    target_artifact_url : str (Reactflow will take it from the previous step)
    clip_vector_path : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)

    try:
        # Fetch target GeoDataFrame from MinIO
        with client.get_object(client_id, target_artifact_url) as target_response:
            target_gdf = gpd.read_file(io.BytesIO(target_response.read()))
        
        with client.get_object(client_id, clip_vector_path) as target_response:
            clip_gdf = gpd.read_file(io.BytesIO(target_response.read()))


        
        
        if not isinstance(target_gdf, gpd.GeoDataFrame) or not isinstance(clip_gdf, gpd.GeoDataFrame):
            raise TypeError("Both target and clip data must be GeoDataFrames.")
        
        # Auto-reproject clip_gdf to target CRS if needed
        if target_gdf.crs != clip_gdf.crs:
            clip_gdf = clip_gdf.to_crs(target_gdf.crs)

        # Perform clipping
        clipped_data = gpd.clip(target_gdf, clip_gdf)

        # Save clipped output
        if store_artifact:
            save_feature(
                client_id=client_id,
                store_artifact=store_artifact,
                gdf=clipped_data,
                file_path=file_path,
                config_path=config
            )
        else:
            print("Data not saved. Set store_artifact to 'minio' or 'local' to save the data.")
            print("Clipping completed successfully.")

        return

    except Exception as e:
        raise RuntimeError(f"Error while performing clip operation: {e}")
