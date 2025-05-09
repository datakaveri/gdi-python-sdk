import geopandas as gpd
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature
import io
from shapely.geometry import box

def create_voronoi_diagram(
    config: str,
    client_id: str,
    input_artifact_url: str,
    extend_artifact_url: str = None,
    store_artifact: str = None,
    file_path: str = None,
    tolerance: float = 0.0,
    only_edges: bool = False
) -> gpd.GeoDataFrame:
    """
    Reads geospatial point data, computes Voronoi polygons, and saves the processed data back to MinIO or locally, while adding point attributes to the polygons. In editor it will be renamed as create-voronoi-diagram.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    input_artifact_url : str (Reactflow will take it from the previous step)
    extend_artifact_url : str (Reactflow will take it from the previous step, This parameter will be optoinal)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    tolerance : float (Reactflow will translate it as input, This parameter will be optional)
    only_edges : enum [True, False] (Reactflow will translate it as input, This parameter will be optional)
    """
    client = connect_minio(config, client_id)
    
    try:
        with client.get_object(client_id, input_artifact_url) as response:
            points_gdf = gpd.read_file(io.BytesIO(response.read()))
        
        if not all(points_gdf.geometry.geom_type == "Point"):
            raise ValueError("Input file must contain only Point geometries.")
        
        extend_to = None
        if extend_artifact_url:
            with client.get_object(client_id, extend_artifact_url) as ext_response:
                extend_gdf = gpd.read_file(io.BytesIO(ext_response.read()))
                extend_to = extend_gdf.geometry.unary_union

        # Generate Voronoi polygons based on the points
        voronoi_polygons = points_gdf.geometry.voronoi_polygons(tolerance=tolerance, extend_to=extend_to, only_edges=only_edges)
    
        # Create a GeoDataFrame for the Voronoi polygons
        voronoi_gdf = gpd.GeoDataFrame(geometry=voronoi_polygons, crs=points_gdf.crs)

        # Perform spatial join to add point attributes to the polygons
        joined_gdf = gpd.sjoin(voronoi_gdf, points_gdf, how="left", predicate='intersects')
        
        # Group by the geometry of the Voronoi polygons and concatenate point attributes
        for col in points_gdf.columns:
            if col != 'geometry':  # Avoid processing geometry column
                # Group by the geometry of Voronoi polygons and concatenate point attribute values
                joined_gdf[col] = joined_gdf.groupby('geometry')[col].transform(lambda x: ', '.join(x.astype(str)))
                
        # save the dataframe to temporary pickle file
        joined_gdf.to_pickle("temp.pkl")
        
    except Exception as e:
        raise e
    
    if store_artifact:
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=joined_gdf, file_path=file_path, config_path=config)

    else:
        print("Data not saved. Set store_artifact to minio/local to save the data to minio or locally.")
        print("Computed geometry successfully")
        # print(gdata)

    return 


# create_voronoi_diagram(
#     config='config.json',
#     client_id='c669d152-592d-4a1f-bc98-b5b73111368e',
#     input_artifact_url='School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.pkl',
#     extend_artifact_url=None,
#     store_artifacts=minio,
#     file_path='vector_voronoi/Voronoi_school_v9.pkl',
# )

