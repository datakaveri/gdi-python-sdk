import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature
import io


def compute_geometry_measures(config: str, client_id: str, artifact_url: str, store_artifact: str, file_path: str = None)->None:
    """
    Reads geospatial data from MinIO, computes geometry measures, and optionally saves the processed data back to MinIO or save locally.In editor it will be renamed as compute-geometry.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """
    client = connect_minio(config, client_id)

    try:
        with client.get_object(client_id, artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))
        
        if gdf.crs is None:
            # print("Warning: No CRS found! Assuming EPSG:4326 (WGS 84).")
            gdf.set_crs(epsg=4326, inplace=True)
        
        gdf = gdf.to_crs(epsg=7755)
        # print("Reprojected to EPSG:7755.")

        geometry_type = gdf.geometry.iloc[0].geom_type
        # print(f"Geometry type identified: {geometry_type}")

        if geometry_type == "Point":
            print("Geometry is Point. No area or perimeter computation needed.")
        elif geometry_type in ["LineString", "MultiLineString"]:
            gdf["length_m"] = gdf.length
            # print("Geometry is Line or MultiLine. Computed length in meters.")
        elif geometry_type in ["Polygon", "MultiPolygon"]:
            gdf["area_sq_m"] = gdf.area
            gdf["perimeter_m"] = gdf.length
            # print("Geometry is Polygon. Computed area and perimeter in meters.")
        else:
            print("Unsupported geometry type.")
    except Exception as e:
        raise e
    
    if store_artifact:
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=gdf, file_path=file_path, config_path=config)

    else:
        print("Data not saved. Set store_artifact to minio/local to save the data to minio or locally.")
        print("Computed geometry successfully")
        # print(gdata)

    return 

# compute_geometry_measures('config.json', 'c669d152-592d-4a1f-bc98-b5b73111368e', 'SurfaceWater_Varanasi_bfab0cfd-93ca-426e-8a2c-addd67e74b30.geojson', minio, 'processed_geometry/Processed_SurfaceWater.geojson')
