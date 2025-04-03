import os
import uuid
import pickle
import geopandas as gpd
from common.minio_ops import connect_minio

def make_clip(
    config: str,
    client_id: str,
    target_artifact_url: str,
    clip_artifact_url: str,
    store_artifacts: bool = False,
    file_path: str = None
) -> dict:
    """
    Clip a target GeoDataFrame with another GeoDataFrame (clip layer),
    both downloaded from MinIO. Optionally upload the clipped result back to MinIO.In editor it will be renamed as clip-vector.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    target_artifact_url : str (Reactflow will translate it as input)
    clip_artifact_url : str (Reactflow will translate it as input)
    store_artifacts : enum [True, False] (Reactflow will translate it as input)
    file_path : str (Reactflow will ignore this parameter)
    """
    
    client = connect_minio(config, client_id)
    temp_target_path = "temp_target.pkl"
    temp_clip_path = "temp_clip.pkl"
    
    with client.get_object(client_id, target_artifact_url) as resp:
        with open(temp_target_path, "wb") as f:
            f.write(resp.read())
    
    with client.get_object(client_id, clip_artifact_url) as resp:
        with open(temp_clip_path, "wb") as f:
            f.write(resp.read())
    
    with open(temp_target_path, "rb") as f:
        target_data = pickle.load(f)
    with open(temp_clip_path, "rb") as f:
        clip_data = pickle.load(f)
    
    if not isinstance(target_data, gpd.GeoDataFrame) or not isinstance(clip_data, gpd.GeoDataFrame):
        raise TypeError("Both target and clip data must be GeoDataFrames.")

    if target_data.crs != clip_data.crs:
        raise ValueError("CRS mismatch: Target and Clip GeoDataFrames must have the same CRS.")

    clipped_data = gpd.clip(target_data, clip_data)
    local_clipped_path = "temp_clipped.pkl"
    clipped_data.to_pickle(local_clipped_path)

    result = {"clipped_file": local_clipped_path, "message": "Clip complete."}
    
    if store_artifacts:
        if not file_path:
            file_path = f"{uuid.uuid4().hex}.pkl"
        client.fput_object(client_id, file_path, local_clipped_path)
        os.remove(local_clipped_path)
        result["clipped_file"] = file_path
        result["message"] = "Clipped data uploaded to MinIO."
        print(file_path)
        
    else:
        print("Data not saved. Set store_artefacts to True to save the data to minio.")
        print("Data buffered successfully")
    
    os.remove(temp_target_path)
    os.remove(temp_clip_path)

    return result
