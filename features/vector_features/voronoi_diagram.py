import geopandas as gpd
from common.minio_ops import connect_minio
import pickle as pkl
import os
import uuid
from shapely.geometry import box

def create_voronoi_diagram(config: str, client_id: str, input_artefact_url: str, extend_artefact_url: str = None, store_artefacts: bool = False, file_path: str = None, tolerance: float = 0.0, only_edges: bool = False) -> gpd.GeoDataFrame:
    """
    Reads geospatial point data from MinIO, computes Voronoi polygons, and saves the processed data back to MinIO, while adding point attributes to the polygons.

    Parameters
    ----------
    config : str (Node red will translate it as input)
    client_id : str (Node red will translate it as input)
    input_artefact_url : str (Node red will translate it as input)
    extend_artefact_url : str (Node red will translate it as input)
    store_artefacts : enum [True, False] (Node red will translate it as input)
    file_path : str (Node red will ignore this parameter)
    tolerance : float (Node red will translate it as input)
    only_edges : enum [True, False] (Node red will translate it as input)
    """
    client = connect_minio(config, client_id)
    
    try:
        with client.get_object(client_id, input_artefact_url) as response:
            points_gdf = pkl.loads(response.read())
        
        if not all(points_gdf.geometry.geom_type == "Point"):
            raise ValueError("Input file must contain only Point geometries.")
        
        extend_to = None
        if extend_artefact_url:
            with client.get_object(client_id, extend_artefact_url) as ext_response:
                extend_gdf = pkl.loads(ext_response.read())
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
    
    if store_artefacts:
        if not file_path:
            file_path = f"{uuid.uuid4()}.pkl"
        try:
            client.fput_object(client_id, file_path, "temp.pkl")
            os.remove("temp.pkl")
            print(f"File stored successfully at: {file_path}")
        except Exception as e:
            raise Exception(f"Error while saving file: {e}")
    else:
        print("Data not saved. Set store_artefacts to True to save the data to MinIO.")
    
    print(f"Voronoi diagram generated successfully in {file_path}")



# create_voronoi_diagram(
#     config='config.json',
#     client_id='c669d152-592d-4a1f-bc98-b5b73111368e',
#     input_artefact_url='School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.pkl',
#     extend_artefact_url=None,
#     store_artefacts=True,
#     file_path='vector_voronoi/Voronoi_school_v9.pkl',
# )

