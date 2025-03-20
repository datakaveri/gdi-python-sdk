import sys
import numpy as np
import os
import uuid
import pickle
import geopandas as gpd
from shapely.geometry import Point, Polygon
from scipy.spatial import Delaunay
from common.minio_ops import connect_minio

if "numpy._core.numeric" not in sys.modules:
    try:
        import numpy.core.numeric
        sys.modules["numpy._core.numeric"] = numpy.core.numeric
    except ImportError:
        pass


def _delaunay_patch():
    def delaunay_triangles(geoseries, **kwargs):
        kwargs.pop('tol', None)
        coords = np.array([(pt.x, pt.y) for pt in geoseries if isinstance(pt, Point)])
        if len(coords) < 3:
            raise ValueError("Not enough points for triangulation.")
        tri = Delaunay(coords, **kwargs)
        return gpd.GeoSeries([Polygon(coords[simplex]) for simplex in tri.simplices], crs=geoseries.crs)
    
    if not hasattr(gpd.GeoSeries, "delaunay_triangles"):
        gpd.GeoSeries.delaunay_triangles = delaunay_triangles


def make_delaunay_triangles(config: str, client_id: str, artifact_url: str, store_artifacts: bool = False, file_path: str = None, **kwargs) -> dict:
    """
    Function to download a pickled GeoDataFrame/GeoSeries from MinIO, perform Delaunay triangulation,
    and optionally upload the triangulation back to MinIO.
    
    Parameters
    ------------
    config : str (Node red will translate it as input)
    client_id : str (Node red will translate it as input)
    artifact_url : str (Node red will translate it as input)
    store_artifacts : enum [True, False] (Node red will translate it as input)
    file_path : str (Node red will ignore this parameter)
    **kwargs : dict (Node red will ignore this parameter)
    """

    client = connect_minio(config, client_id)
    _delaunay_patch()

    temp_input_pkl = "temp_input.pkl"
    
    with client.get_object(client_id, artifact_url) as response:
        with open(temp_input_pkl, "wb") as f:
            f.write(response.read())
    
    with open(temp_input_pkl, "rb") as f:
        points_data = pickle.load(f)
    
    if isinstance(points_data, gpd.GeoDataFrame):
        points_geoseries = points_data.geometry
    elif isinstance(points_data, gpd.GeoSeries):
        points_geoseries = points_data
    else:
        raise TypeError("Input data must be a GeoDataFrame or GeoSeries.")

    if points_geoseries.empty or len(points_geoseries) < 3:
        raise ValueError("Delaunay requires at least 3 points.")

    triangulation = points_geoseries.delaunay_triangles(**kwargs)

    temp_triangulation_pkl = "temp_triangulation.pkl"
    triangulation.to_pickle(temp_triangulation_pkl)

    result = {"triangulation_file": temp_triangulation_pkl, "message": "Triangulation complete."}
    
    if store_artifacts:
        if not file_path:
            file_path = f"{uuid.uuid4().hex}.pkl"
        client.fput_object(client_id, file_path, temp_triangulation_pkl)
        os.remove(temp_triangulation_pkl)
        result["triangulation_file"] = file_path
        result["message"] = "Triangulation uploaded to MinIO."
        print(file_path)
        
    else:
        print("Data not saved. Set store_artefacts to True to save the data to minio.")
        print("Data buffered successfully")
    
    os.remove(temp_input_pkl)
    return result
