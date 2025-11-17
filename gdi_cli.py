import click
from auth.token_gen import TokenGenerator

from features.vector_features.get_re import ResourceFetcher
from features.vector_features.count_features import count_features
from features.vector_features.download_features import download_features
from features.vector_features.buffer import make_buffer
from features.vector_features.intersection import make_intersection
from features.vector_features.gcode import list_features
from features.vector_features.compute_geo import compute_geometry_measures
from features.vector_features.ReduceToImage import reduce_to_image
from features.vector_features.optimalRoute import compute_optimal_route
from features.vector_features.voronoi_diagram import create_voronoi_diagram
from features.vector_features.clip_data import make_clip
from features.vector_features.delaunay_triangles import make_delaunay_triangles
from features.vector_features.bbox_clip_feature import bbox_clip_feature
from features.vector_features.vector_format_convert import convert_vector_format
from features.vector_features.simplify_geom import simplify_geometry_DP
from features.vector_features.clustering import generate_clusters



from features.raster_features.search_cat import search_stac
from features.raster_features.get_data import get_assets
from features.raster_features.flood_fill import flood_fill
from features.raster_features.isometric_lines import isometric_lines
from features.raster_features.compute_slope import compute_slope
from features.raster_features.NDVI import compute_ndvi 
from features.raster_features.clip_raster import clip_raster
from features.raster_features.merge_rasters import merge_rasters  
from features.raster_features.download_raster import download_rasters_artifact
from features.raster_features.bbox_clip_raster import bbox_clip_raster
from features.raster_features.local_correlation import compute_local_correlation_5x5
from features.raster_features.reduce_to_feature import extract_raster_to_vector
from features.raster_features.band_extraction import band_extraction
from features.raster_features.raster_format_convert import convert_raster_format
from features.raster_features.compute_aspect import compute_aspect
from features.raster_features.compute_hillshade import compute_hillshade
from features.raster_features.canny_edge import compute_canny_edge
from features.raster_features.hough_transform import get_hough_transform

from common.minio_ops import get_ls

def gen_token(client_id, client_secret, role):
    token_generator = TokenGenerator(client_id, client_secret, role)
    auth_token = token_generator.generate_token()
    return auth_token


def get_resource(client_id, client_secret, role, resource_id, save_object, config_path, file_path):
   
    fetcher = ResourceFetcher(client_id, client_secret, role, resource_id)
    resource_data = fetcher.fetch_resource_data(resource_id, save_object, config_path, file_path)
    return resource_data




@click.command()
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--client-secret', required=True, help="Client secret for authentication.")
@click.option('--role', required=True, help="Role for the token.", default = "consumer")
def generate_token(client_id, client_secret, role):
    """Generate an authentication token."""
    token = gen_token(client_id, client_secret, role)
    click.echo(f"Generated Token: {token}")


@click.command()
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--client-secret', required=True, help="Client secret for authentication.")
@click.option('--role', required=True, help="Role for the token.", default = "consumer")
@click.option('--resource-id', required=True, help="ID of the resource to fetch.")
@click.option('--store-artifact', default='minio',  help="Save the fetched object to Minio or locally. set it as local or minio")
@click.option('--config-path',required=False, default="./config.json",  help="Path to the Minio configuration file. Only if you are saving the object.")
@click.option('--file-path', help="Path to save the fetched object.")


def get_vector_data(client_id, client_secret, role, resource_id, store_artifact, config_path, file_path):
    """Fetch resource data."""
    resource_data = get_resource(client_id, client_secret, role, resource_id, store_artifact, config_path, file_path)
    

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="used as the bucket name.")
@click.option('--artifact-url', required=True, help="URL of the artifact to count features.")
def features_count(config_path, client_id, artifact_url):
    """Count the features in the artifact."""
    count = count_features(config_path, client_id, artifact_url)
    click.echo(f"Feature Count: {count}")


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="used as the bucket name.")
def ls_objects(config_path, client_id):
    """List objects in the bucket."""
    get_ls(config_path, client_id)


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to download.")
@click.option('--save-as', help="Save the fetched object to Minio as the given file name.")
def download_vector_features(config_path, client_id, artifact_url, save_as):
    """Download the artifact."""
    download_features(config_path, client_id, artifact_url, save_as)


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to buffer.")
@click.option('--buffer-d', required=True, help="Buffer distance.")
@click.option('--store-artifact', default='minio', help="Store the buffered artifact. Set it to local/minio.")
@click.option('--file-path', help="Path to save the buffered artifact.")
def create_buffer(config_path, client_id, artifact_url, buffer_d, store_artifact, file_path):
    """Buffer the artifact."""
    make_buffer(config_path, client_id, artifact_url, buffer_d, store_artifact, file_path)


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--left-feature', required=True, help="URL of the first artifact.")
@click.option('--right-feature', required=True, help="URL of the second artifact.")
@click.option('--store-artifact', default='minio', help="Store the intersected artifact.Set it to local/minio.")
@click.option('--file-path', help="Path to save the intersected artifact.")
def create_intersection(config_path, client_id, left_feature, right_feature, store_artifact, file_path):
    """Intersect the artifacts."""
    make_intersection(config_path, client_id, left_feature, right_feature, store_artifact, file_path)


@click.command()
@click.option('--location', required=True, help="Name of the place. City, State, Country etc.")
def list_data(location):
    """List data for a location."""
    list_features(location)
    

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to download.")
@click.option('--store-artifact', default='minio', help="Store the compute geomtery artifact. Set it to local/minio.")
@click.option('--file-path', help="Path to save the compute geomtery artifact.")
def compute_geometry(config_path, client_id, artifact_url, store_artifact, file_path):
    """
    Reads geospatial data from MinIO, computes geometry measures, and optionally saves the processed data back to MinIO.
    """
    compute_geometry_measures(config_path, client_id, artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact.")
@click.option('--attribute', required=True, help="Attribute to reduce.")
@click.option('--grid-size', required=True, type=float, help="Size of the grid.")
@click.option('--reducer', required=True, help="Reducer operation.")
@click.option('--store-artifact', default='minio', help="Store reduced raster. Set it to local/minio.")
@click.option('--file-path', help="Path to save the reduced raster artifact.")
def reduce_to_raster(config_path, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path):
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    reduce_to_image(config_path, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path)
    click.echo(file_path)

@click.command(name="create-optimal-route")
@click.option('--config-path', required=False, default="./config.json", help="Path to MinIO config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--artifact-url', required=True, help="URL to road network object name in MinIO.")
@click.option('--points-filepath', required=True, help="Local path to the sample points (GeoJSON/Shapefile...).")
@click.option('--store-artifact', default='minio', help="Store final route & points. Set it to local/minio.")
@click.option('--file-path', help="MinIO object name to store final route (GeoJSON).")
def create_optimal_route(config_path, client_id, artifact_url, points_filepath, store_artifact, file_path):
    """
    Compute a TSP-based optimal route by:
      - downloading a pickled road network from MinIO,
      - reading points from local disk,
      - building & solving TSP,
      - optionally storing route & points back to MinIO.
    """
    compute_optimal_route(config_path, client_id, artifact_url, points_filepath, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--input-artifact-url', required=True, help="URL of the point artifact to generate voronoi polygon.")
@click.option('--extend-artifact-url', default=None, help="URL of the artifact to define extend of output.")
@click.option('--tolerance', type=float, default=0.0, help="Snap input vertices together if their distance is less than this value.")
@click.option('--edges', type=bool, default=False, help="If set to True, the diagram will return LineStrings instead of Polygons.")
@click.option('--store-artifact', default='minio', help="Store the voronoi polygons. Set it to local/minio.")
@click.option('--file-path', default=None, help="Path to save the voronoi artifact.")
def create_voronoi(config_path, client_id, input_artifact_url, extend_artifact_url, tolerance, edges, store_artifact, file_path):
    """Create Voronoi diagram based on input artifact"""
    create_voronoi_diagram(config_path, client_id, input_artifact_url, extend_artifact_url, store_artifact, file_path, tolerance, edges)

@click.command(name="clip-vector")
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--target-artifact-url', required=True, help="URL of the target artifact.")
@click.option('--clip-artifact-url', required=True, help="URL of the clip artifact")
@click.option('--store-artifact', default='minio',  help="Store the clipped artifact. Set it to local/minio")
@click.option('--file-path', default="None", help="Path to save the clipped artifact")
def clip_vector(config_path, client_id, target_artifact_url, clip_artifact_url, store_artifact, file_path):
    """
    Clip a target feature to the extent of the clip feature,
    """
    make_clip(config_path, client_id, target_artifact_url, clip_artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to minio config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="URL of the input artifact.")
@click.option('--store-artifact', default='minio', help="Store delaunay artifact. Set it to local/minio")
@click.option('--file-path', default="None", help="Path to save the delaunay artifact")
@click.option('--qhull-options', default="", help="Additional Qhull options for scipy.spatial.Delaunay.")
def create_delaunay_triangles(config_path, client_id, artifact_url, store_artifact, file_path, qhull_options):
    """
    Create Delaunay triangles from a pickled GeoDataFrame/GeoSeries stored in MinIO,
    optionally uploading the result back to MinIO.
    """
    extra_kwargs = {"qhull_options": qhull_options} if qhull_options else {}
    make_delaunay_triangles(config_path, client_id, artifact_url, store_artifact, file_path,**extra_kwargs)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--target-artifact-url', required=True, help="URL of the target artifact.")
@click.option('--clip-vector-path', required=True, help="Local filepath of the polygon GeoJSON used as the bbox to clip.")
@click.option('--store-artifact', default='minio',  help="Store the clipped artifact. Set it to local/minio")
@click.option('--file-path', default="None", help="Path to save the clipped artifact")
def bbox_feature_clip(config_path, client_id, target_artifact_url, clip_vector_path, store_artifact, file_path):
    """
    Clip a target feature to the extent bbox, input as a geojson from local path.
    """
    bbox_clip_feature(config_path, client_id, target_artifact_url, clip_vector_path, store_artifact, file_path)


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--input-vector', required=True, help="URL of the input vector artifact.")
@click.option('--input-artifact', required=True, help="Store the vector artifact. Set it to local/minio.")
@click.option('--file-path', default="None", help="Path to save the vector artifact.")
@click.option('--store-artifact', default='minio', help="Store the vector artifact. Set it to local/minio.")

def convert_vector(config_path, client_id, store_artifact, input_vector, file_path, input_artifact):
    """
    Convert vector data to a different format (GeoJSON, Shapefile, GPKG) and save it.
    """
    convert_vector_format(config_path, client_id, input_vector, input_artifact, file_path, store_artifact)

@click.command("simplify-geometry")
@click.option("--config-path", required=True, type=str, help="Path to config.json")
@click.option("--client-id", required=True, type=str, help="Client or bucket name")
@click.option("--artifact-url", required=True, type=str, help="Url of input vector artifact")
@click.option("--store-artifact", required=True, type=str, help="Store the vector artifact. Set it to local/minio.")
@click.option("--file-path", required=False, type=str, help="Path to save the output artifact.")
@click.option("--tolerance", required=False, default=1.0, type=float, help="Simplification tolerance (default=1.0)")
@click.option("--preserve-topology", required=False, default=True, type=bool, help="Preserve topology for polygons (default=True)")
def simplify_geometry(config_path, client_id, artifact_url, store_artifact, file_path, tolerance, preserve_topology):
    """
    Simplify vector geometries (LineString or Polygon only) using shapely.simplify().
    """
    simplify_geometry_DP(
        config=config_path,
        client_id=client_id,
        artifact_url=artifact_url,
        store_artifact=store_artifact,
        file_path=file_path,
        tolerance=tolerance,
        preserve_topology=preserve_topology
    )

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name or project ID.")
@click.option('--artifact-url', required=True, help="Url of input vector artifact.")
@click.option('--store-artifact', default='minio', help="Store the vector artifact. Set it to local/minio..")
@click.option('--n-clusters', default=20, show_default=True, help="Number of KMeans clusters to form.")
@click.option('--file-path', default=None, help="Output file path to save the clustered result.")
def kmeans_clustering(config_path, client_id, artifact_url, store_artifact, n_clusters, file_path):
    """
    Perform KMeans clustering on input vector datasets and an attribute denoting the cluster number each feature in input vector belong to.
    Input vectors are fetched from MinIO and output can be saved locally or uploaded back.
    """
    generate_clusters(
        config=config_path,
        client_id=client_id,
        artifact_url=artifact_url,
        store_artifact=store_artifact,
        n_clusters=n_clusters,
        file_path=file_path
    )





















# Raster feature utilities

@click.command()
@click.option('--collection-ids', required=True, help="Collection ID to search and access a specific collection")
def search_cat(collection_ids):
    '''Search for a given collection'''
    search_stac(collection_ids)

@click.command()
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--client-secret', required=True, help="Client secret for authentication.")
@click.option('--role', required=True, help="Role for the token.")
@click.option('--collection-ids', required=True, help="Collection ID to search and access a specific collection")
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--store-artifact', default='minio', help="Store downloaded STAC assets. Set it to local/minio")
@click.option('--dir-path', help="Specify folder name to save the downloaded assets")
@click.option('--item-id', help="STAC item if to be downloaded, if not specfied all assets in the collection id provided will be downloaded.")
def get_raster_data(client_id, client_secret, role, collection_ids, config_path, store_artifact, dir_path, item_id):
    '''Download a particular assets or all the assets in the collections specified.'''
    get_assets(client_id, client_secret, role, collection_ids, config_path,store_artifact, dir_path, item_id)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name of the input DEM.")
@click.option('--threshold', required=True, type=float, help="threshold elevation upto which it is inundated.")
@click.option('--store-artifact', default='minio', help="Store flood inundated raster. Set it to local/minio")
@click.option('--file-path', help="MinIO key name for flood fill layer. If not provided, a UUID name is used.")
def flood_fill_model(config_path, client_id, artifact_url, threshold, store_artifact, file_path):
    '''Create flood inundated raster based on input DEM and threshold value'''
    flood_fill(config_path, client_id, artifact_url, threshold, store_artifact, file_path)

    
@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name of the input raster.")
@click.option('--interval', required=True, type=float, help="Specify intervals based on which defines the number of levels for isometric lines generated.")
@click.option('--store-artifact', default='minio', help="Store isometric lines artifact. Set it to local/minio")
@click.option('--file-path', help="Path for for saving isometric lines generated. If not provided, a UUID name is used.")
def generate_isometric_lines(config_path, client_id, artifact_url, interval, store_artifact, file_path):
    '''Create flood inundated raster based on input DEM and threshold value'''
    isometric_lines(config_path, client_id, artifact_url, interval, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name for the DEM GeoTIFF (or COG).")
@click.option('--store-artifact', default='minio', help="Store generated slope raster. Set it to local/minio")
@click.option('--file-path', help="Path for for saving slope raster generated. If not provided, a UUID name is used.")
def generate_slope(config_path, client_id, artifact_url, store_artifact, file_path):
    '''Create slope raster for the input DEM raster'''
    compute_slope(config_path, client_id, artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name for the DEM GeoTIFF (or COG).")
@click.option('--store-artifact', default='minio', help="Store generated aspect raster. Set it to local/minio")
@click.option('--file-path', help="Path for saving aspect raster generated. If not provided, a UUID name is used.")
def generate_aspect(config_path, client_id, artifact_url, store_artifact, file_path):
    '''Create aspect raster for the input DEM raster'''
    compute_aspect(config_path, client_id, artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name for the DEM GeoTIFF (or COG).")
@click.option('--store-artifact', default='minio', help="Store generated hillshade raster. Set it to local/minio")
@click.option('--file-path', help="Path for for saving hillshade raster generated. If not provided, a UUID name is used.")
def generate_hillshade(config_path, client_id, artifact_url, store_artifact, file_path):
    '''Create hillshade raster for the input DEM raster'''
    compute_hillshade(config_path, client_id, artifact_url, store_artifact, file_path)


@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--red-artifact-url', required=True, help="MinIO object name for the Red band GeoTIFF.")
@click.option('--nir-artifact-url', required=True, help="MinIO object name for the NIR band GeoTIFF.")
@click.option('--store-artifact', default='minio', help="Store generated NDVI raster. Set it to local/minio")
@click.option('--file-path', help="Path for saving NDVI raster generated. If not provided, a UUID name is used.")
def generate_ndvi(config_path, client_id, red_artifact_url, nir_artifact_url, store_artifact, file_path):
    '''Create NDVI raster from the input Red and NIR band rasters'''
    compute_ndvi(config_path, client_id, red_artifact_url, nir_artifact_url, store_artifact, file_path)


@click.command()
@click.option('--config-path', default="./config.json",help="Path to the MinIO config file.")
@click.option('--client-id', required=True,help="Bucket name (client‑id).")
@click.option('--raster-key', required=True,help="Object key of the raster (COG/GeoTIFF) to clip.")
@click.option('--geojson-key', required=True,help="Object key of the polygon GeoJSON used as the mask.")
@click.option('--store-artifact', default='minio',  help="Save the fetched object to Minio or locally. set it as local or minio")
@click.option('--file-path',help="Optional path to save output file. If not provided, a UUID name is used.")
def raster_clip(config_path, client_id, raster_key, geojson_key, store_artifact, file_path):
    """Clip a raster with a GeoJSON polygon and output one COG."""
    final_path = clip_raster(config_path, client_id, raster_key, geojson_key, store_artifact, file_path)

@click.command()
@click.option('--config-path', default="./config.json",help="Path to the MinIO config file.")
@click.option('--client-id', required=True,help="Bucket name (client‑id).")
@click.option('--prefix', required=True,help="MinIO folder prefix that holds the COG TIFs to merge.")
@click.option('--store-artifact', default='minio',  help="Save the fetched object to Minio or locally. set it as local or minio")
@click.option('--file-path',help="Destination key for the final COG in MinIO ""(only used when --store-artifact=minio).")
def rasters_merge(config_path, client_id, prefix, store_artifact, file_path):
    """Merge multiple rasters into a single COG and output its path."""
    final_path = merge_rasters(config_path, client_id, prefix, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to download.")
def download_raster(config_path, client_id, artifact_url, save_as):
    """Generate presigned url to download raster artifact."""
    download_rasters_artifact(config_path, client_id, artifact_url, save_as)

@click.command()
@click.option('--config-path', default="./config.json",help="Path to the MinIO config file.")
@click.option('--client-id', required=True,help="Bucket name (client‑id).")
@click.option('--raster-key', required=True,help="Object key of the raster (COG/GeoTIFF) to clip.")
@click.option('--vector-path', required=True,help="Local filepath of the polygon GeoJSON used as the mask.")
@click.option('--store-artifact', default='minio',  help="Save the fetched object to Minio or locally. set it as local or minio")
@click.option('--file-path',help="Optional path to save output file. If not provided, a UUID name is used.")
def bbox_raster_clip(config_path, client_id, raster_key, vector_path, store_artifact, file_path):
    """Clip a raster with a GeoJSON polygon and output one COG."""
    final_path = bbox_clip_raster(config_path, client_id, raster_key, vector_path, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--x', required=True, help="first raster key in MinIO.")
@click.option('--y', required=True, help="second raster key in MinIO.")
@click.option('--chunk-size', default=500, type=int, help="Chunk size for reading/writing blocks.")
@click.option('--store-artifact', default='minio', help="Store generated correlation raster. Set it to local/minio")
@click.option('--file-path', help="Path for for saving correlation raster generated. If not provided, a UUID name is used.")
def generate_local_correlation(config_path, client_id, x, y, chunk_size, store_artifact, file_path):
    """
    Compute a 5x5 local correlation between two rasters, where the window size is fixed as 5.
    """  
    compute_local_correlation_5x5(config_path, client_id, x, y, chunk_size, store_artifact, file_path)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--raster-artifact-url', required=True, help="Path to raster file in MinIO.")
@click.option('--vector-artifact-url', required=True, help="Path to vector file (GeoJSON or GPKG).")
@click.option('--reducer', required=True, type=click.Choice(['mean', 'min', 'max', 'count', 'sum']), help="Reducer operation to apply.")
@click.option('--attribute', required=True, help="Name of attribute to store extracted value in output.")
@click.option('--store-artifact', default='minio', help="Set to 'minio' to upload result, or 'local' to save locally.")
@click.option('--file-path', default=None, help="Optional path to save output file. If not provided, a UUID name is used.")
def reduce_to_feature(config_path, client_id, raster_artifact_url, vector_artifact_url, reducer, attribute, store_artifact, file_path):
    """
    Extract raster values into vector features using spatial join with a specified reducer.
    """
    extract_raster_to_vector(config_path, client_id, raster_artifact_url, vector_artifact_url, reducer, attribute, store_artifact, file_path)

@click.command()
@click.option('--asset-list', required=True, help="String of paths to geotiff separated by delimiter $")
@click.option('--item-key', required=True, help="Item title as key")
@click.option('--asset-key', required=True, help="Keyword in stac asset to enable search in string of paths")
def extract_band_path(asset_list, item_key, asset_key):
    """
    To extract a band/ raster image path with item id and asset keyword from string of paths separated by delimiter $.
    """
    band_extraction(asset_list, item_key, asset_key)

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--input-raster', required=True, help="Path or MinIO key of the input raster file.")
@click.option('--input-artifact', required=True, help="Where the input raster is stored: local or minio.")
@click.option('--file-path', required=True, help="Path or MinIO key to save the converted raster file.")
@click.option('--store-artifact', default='minio', help="Where to store the converted raster file: local or minio.")
def convert_raster(config_path, client_id, store_artifact, input_raster, file_path, input_artifact):
    """
    Convert raster data to a different format (GeoTIFF, IMG) and save it.
    """
    convert_raster_format( 
        config_path=config_path,
        client_id=client_id,
        input_path=input_raster,
        input_store=input_artifact,
        output_path=file_path,
        output_store=store_artifact
    )

@click.command()
@click.option('--config-path', required=False, default="./config.json", help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name for the input raster.")
@click.option('--store-artifact', default='minio', help="Store generated edge raster. Set it to local/minio.")
@click.option('--file-path', help="Path for saving edge raster. If not provided, a UUID name is used.")
@click.option('--threshold1', default=100, help="Lower threshold for Canny edge detection.")
@click.option('--threshold2', default=200, help="Upper threshold for Canny edge detection.")
def generate_canny_edge(config_path, client_id, artifact_url, store_artifact, file_path, threshold1, threshold2):
    """Perform Canny edge detection on the input raster."""
    compute_canny_edge(config_path, client_id, artifact_url, store_artifact, file_path, threshold1, threshold2)


@click.command()
@click.option("--config", required=True, type=str, help="Path to configuration JSON file")
@click.option("--client-id", required=True, type=str, help="Client ID or MinIO bucket identifier")
@click.option("--artifact-url", required=True, type=str, help="Input raster path or URL in MinIO")
@click.option("--store-artifact", required=True, type=str, help="Where to store the processed artifact (minio/local)")
@click.option("--file-path", required=False, type=str, help="Path to save processed output raster")
# Hough method
@click.option("--method", required=True, type=click.Choice(["line", "circle"]), help="Hough transform type")
# Optional parameters for line detection
@click.option("--canny-thresh1", default=100, type=int, help="Canny threshold 1 (only for line method)")
@click.option("--canny-thresh2", default=200, type=int, help="Canny threshold 2 (only for line method)")
@click.option("--hough-thresh", default=50, type=int, help="Hough line threshold (only for line method)")
@click.option("--min-line-length", default=10, type=int, help="Minimum line length (only for line method)")
@click.option("--max-line-gap", default=10, type=int, help="Maximum allowed gap between line segments (only for line method)")
# Optional parameters for circle detection
@click.option("--dp", default=1, type=float, help="Inverse ratio of accumulator resolution to image resolution (only for circle method)")
@click.option("--min-dist", default=20, type=int, help="Minimum distance between circle centers (only for circle method)")
@click.option("--param1", default=100, type=int, help="Upper threshold for internal Canny edge detector (only for circle method)")
@click.option("--param2", default=30, type=int, help="Threshold for center detection (only for circle method)")
@click.option("--min-radius", default=0, type=int, help="Minimum circle radius (only for circle method)")
@click.option("--max-radius", default=0, type=int, help="Maximum circle radius (only for circle method)")
def generate_hough_transform(config, client_id, artifact_url, store_artifact, file_path, method,
        canny_thresh1, canny_thresh2, hough_thresh, min_line_length, max_line_gap,
        dp, min_dist, param1, param2, min_radius, max_radius):
    """
    Perform Hough Transform - powerful feature extraction technique in image processing and Geographic Information Systems (GIS) used to detect simple, predefined geometric shapes such as lines and circles on raster datasets.
    """
    kwargs = {}
    if method == "line":
        kwargs.update({
            "canny_thresh1": canny_thresh1,
            "canny_thresh2": canny_thresh2,
            "hough_thresh": hough_thresh,
            "min_line_length": min_line_length,
            "max_line_gap": max_line_gap
        })
    elif method == "circle":
        kwargs.update({
            "dp": dp,
            "min_dist": min_dist,
            "param1": param1,
            "param2": param2,
            "min_radius": min_radius,
            "max_radius": max_radius
        })
    get_hough_transform(
        config=config,
        client_id=client_id,
        artifact_url=artifact_url,
        store_artifact=store_artifact,
        file_path=file_path,
        method=method,
        **kwargs
    )

