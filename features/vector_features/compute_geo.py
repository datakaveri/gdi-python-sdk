import geopandas as gpd
from common.minio_ops import connect_minio
import pickle as pkl
import os
import uuid

def compute_geometry_measures(config: str, client_id: str, artifact_url: str, store_artifacts: bool = False, file_path: str = None)->None:
    """
    Reads geospatial data from MinIO, computes geometry measures, and optionally saves the processed data back to MinIO.

    Parameters:
    ----------------------
    config : str (Node red will translate it as input)
    client_id : str (Node red will translate it as input)
    artifact_url : str (Node red will translate it as input)
    store_artifacts : enum [True, False] (Node red will translate it as input)
    file_path : (Node red will ignore this parameter)

    """
    client = connect_minio(config, client_id)

    try:
        with client.get_object(client_id, artifact_url) as response:
            gdf = pkl.loads(response.read())
        
        if gdf.crs is None:
            print("Warning: No CRS found! Assuming EPSG:4326 (WGS 84).")
            gdf.set_crs(epsg=4326, inplace=True)
        
        gdf = gdf.to_crs(epsg=7755)
        print("Reprojected to EPSG:7755.")

        geometry_type = gdf.geometry.iloc[0].geom_type
        print(f"Geometry type identified: {geometry_type}")

        if geometry_type == "Point":
            print("Geometry is Point. No area or perimeter computation needed.")
        elif geometry_type in ["LineString", "MultiLineString"]:
            gdf["length_m"] = gdf.length
            print("Geometry is Line or MultiLine. Computed length in meters.")
        elif geometry_type in ["Polygon", "MultiPolygon"]:
            gdf["area_sq_m"] = gdf.area
            gdf["perimeter_m"] = gdf.length
            print("Geometry is Polygon. Computed area and perimeter in meters.")
        else:
            print("Unsupported geometry type.")

        gdf.to_pickle("temp.pkl")
    except Exception as e:
        raise e
    
    if store_artifacts:
        if not file_path:
            file_path = f"{uuid.uuid4()}.pkl"
        try:
            client.fput_object(client_id, file_path, "temp.pkl")
            os.remove("temp.pkl")
            print(file_path)
        except Exception as e:
            raise Exception(f"Error while saving file: {e}")
    else:
        print("Data not saved. Set store_artifacts to True to save the data to MinIO.")
        print("Data buffered successfully.")

# compute_geometry_measures('config.json', 'c669d152-592d-4a1f-bc98-b5b73111368e', 'SurfaceWater_Varanasi_bfab0cfd-93ca-426e-8a2c-addd67e74b30.pkl', True, 'processed_geometry/Processed_SurfaceWater.pkl')
