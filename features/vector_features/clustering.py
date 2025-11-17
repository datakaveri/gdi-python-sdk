import io
import warnings
import numpy as np
import geopandas as gpd
from sklearn.cluster import KMeans
from shapely.geometry import Point
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

warnings.filterwarnings("ignore")


def generate_clusters(
    config: str,
    client_id: str,
    artifact_url: str,
    store_artifact: str,
    n_clusters: int = 20,
    file_path: str = None
) -> dict:
    """
    Perform KMeans clustering on point-based vector data with default cluster count as 20 and adds attribute to the input vector denoting the cluster number. Optionally upload the clustered result to MinIO or save locally.In editor it will be renamed as kmeans-clustering.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    n_clusters : int (Reactflow will translate it as input, This parameter will be optional)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)


    try:
        # --- Step 1: Read input vector ---
        with client.get_object(bucket_name=client_id, object_name=artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))

        if gdf is None or gdf.empty:
            raise RuntimeError("Input vector is empty or invalid.")

        gdf = gpd.GeoDataFrame(gdf, crs=gdf.crs or "EPSG:4326")

        # --- Step 2: Compute centroids for clustering ---
        centroids = gdf.geometry.centroid
        coords = np.array([[geom.x, geom.y] for geom in centroids])

        # --- Step 3: Perform clustering ---
        n_clusters = min(n_clusters, len(coords))
        gdf['cluster'] = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init=10
        ).fit_predict(coords)

        # --- Step 4: Save clustered output ---
        if store_artifact:
            save_feature(
                client_id=client_id,
                store_artifact=store_artifact,
                gdf=gdf,
                file_path=file_path,
                config_path=config
            )
        else:
            print("Data not saved. Set store_artifact to 'minio' or 'local' to save the data.")
            print("Clustering completed successfully.")

        return

    except Exception as e:
        raise RuntimeError(f"Error while performing clustering operation: {e}")

# # Function call:
# # make_clustering(
# #     config='config.json',
# #     client_id='c669d152-592d-4a1f-bc98-b5b73111368e',
# #     artifact_url='vectors/sample_points.geojson',
# #     store_artifact='minio',
# #     n_clusters=15,
# #     file_path='clustered_output/clustered_points.geojson'
# # )
