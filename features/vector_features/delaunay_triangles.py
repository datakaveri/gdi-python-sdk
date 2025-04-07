import numpy as np
import io
import geopandas as gpd
from shapely.geometry import Point, Polygon
from scipy.spatial import Delaunay
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

# Patch GeoSeries to include a delaunay_triangles method if it doesn't exist
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


def make_delaunay_triangles(
    config: str,
    client_id: str,
    artifact_url: str,
    store_artifact: str,
    file_path: str = None,
    **kwargs
) -> dict:
    """
    Function to perform Delaunay triangulation, and optionally upload the triangulation back to MinIO or save locally.In editor it will be renamed as create-delaunay-triangles.    
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    **kwargs : dict (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)
    _delaunay_patch()

    try:
        with client.get_object(client_id, artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))
        
        if isinstance(gdf, gpd.GeoDataFrame):
            geo_series = gdf.geometry
        elif isinstance(gdf, gpd.GeoSeries):
            geo_series = gdf
        else:
            raise TypeError("Input must be a GeoDataFrame or GeoSeries.")

        if geo_series.empty or len(geo_series) < 3:
            raise ValueError("At least 3 points are required for Delaunay triangulation.")

        triangulation = geo_series.delaunay_triangles(**kwargs)

        if store_artifact:
            save_feature(
                client_id=client_id,
                store_artifact=store_artifact,
                gdf=triangulation,
                file_path=file_path,
                config_path=config
            )
        else:
            print("Data not saved. Set store_artifact to 'minio' or 'local' to save the data.")
            print("Delaunay triangulation completed successfully.")

        return {"status": "success", "triangles": len(triangulation)}

    except Exception as e:
        raise RuntimeError(f"Error during Delaunay triangulation: {e}")


# make_delaunay_triangles(
#     config= 'config.json',
#     client_id= 'c669d152-592d-4a1f-bc98-b5b73111368e ',
#     artifact_url= 'vectors/School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.geojson',
#     store_artifact= 'minio',
#     file_path= 'delaunay/School_var_delaunay_triangles.geojson')