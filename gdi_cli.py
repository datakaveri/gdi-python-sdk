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

from features.raster_features.search_cat import search_stac
from features.raster_features.get_data import get_assets
from features.raster_features.flood_fill import flood_fill
from features.raster_features.isometric_lines import isometric_lines
from features.raster_features.compute_slope import compute_slope
from features.raster_features.NDVI import compute_ndvi 
from features.raster_features.local_correlation import compute_local_correlation_5x5

from common.minio_ops import get_ls

def gen_token(client_id, client_secret, role):
    token_generator = TokenGenerator(client_id, client_secret, role)
    auth_token = token_generator.generate_token()
    return auth_token


def get_resource(client_id, client_secret, role, resource_id, save_object, config_path, file_path):
   
    fetcher = ResourceFetcher(client_id, client_secret, role)
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


def fetch_resource(client_id, client_secret, role, resource_id, store_artifact, config_path, file_path):
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
def download_artifact(config_path, client_id, artifact_url, save_as):
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
    data = list_features(location)
    click.echo(data)

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

def reduce_to_img(config_path, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path):
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    reduce_to_image(config_path, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path)


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
    """Create Voronoi diagram based on input artefact"""
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
def get_stac_assets(client_id, client_secret, role, collection_ids, config_path, store_artifact, dir_path, item_id):
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
@click.option('--red-artifact-url', required=True, help="MinIO object name for the Red band GeoTIFF.")
@click.option('--nir-artifact-url', required=True, help="MinIO object name for the NIR band GeoTIFF.")
@click.option('--store-artifact', default='minio', help="Store generated NDVI raster. Set it to local/minio")
@click.option('--file-path', help="Path for saving NDVI raster generated. If not provided, a UUID name is used.")
def generate_ndvi(config_path, client_id, red_artifact_url, nir_artifact_url, store_artifact, file_path):
    '''Create NDVI raster from the input Red and NIR band rasters'''
    compute_ndvi(config_path, client_id, red_artifact_url, nir_artifact_url, store_artifact, file_path)

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


