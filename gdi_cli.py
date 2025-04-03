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

from common.minio_ops import get_ls

def gen_token(client_id, client_secret, role):
    token_generator = TokenGenerator(client_id, client_secret, role)
    auth_token = token_generator.generate_token()
    return auth_token


def get_resource(client_id, client_secret, role, resource_id, save_object, config_path, file_path):
    """
    Fetch data for a specified resource using the generated token.

    Parameters
    ----------
    client_id : str (Reactflow will translate it as input)
    client_secret : str (Reactflow will translate it as input)
    role : enum [consumer, provider, admin] (Reactflow will translate it as input)
    resource_id : str (Reactflow will translate it as input)
    save_object : enum [True, False] (Reactflow will translate it as input)
    config_path : str (Reactflow will translate it as input)
    file_path : str (Reactflow will ignore this parameter)

    """
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
@click.option('--store-artifact',  help="Save the fetched object to Minio or locally. set it as local or minio")
@click.option('--config-path',  help="Path to the Minio configuration file. Only if you are saving the object.")
@click.option('--file-path', help="Path to save the fetched object.")


def fetch_resource(client_id, client_secret, role, resource_id, store_artifact, config_path, file_path):
    """Fetch resource data."""
    resource_data = get_resource(client_id, client_secret, role, resource_id, store_artifact, config_path, file_path)
    



@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="used as the bucket name.")
@click.option('--artifact-url', required=True, help="URL of the artifact to count features.")
def features_count(config, client_id, artifact_url):
    """Count the features in the artifact."""
    count = count_features(config, client_id, artifact_url)
    click.echo(f"Feature Count: {count}")


@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="used as the bucket name.")
def ls_objects(config, client_id):
    """List objects in the bucket."""
    get_ls(config, client_id)


@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to download.")
@click.option('--save-as', help="Save the fetched object to Minio as the given file name.")
def download_artifact(config, client_id, artifact_url, save_as):
    """Download the artifact."""
    download_features(config, client_id, artifact_url, save_as)


@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to buffer.")
@click.option('--buffer-d', required=True, help="Buffer distance.")
@click.option('--store-artifact', help="Store the buffered artifact. Set it to local/minio.")
@click.option('--file-path', help="Path to save the buffered artifact.")
def create_buffer(config, client_id, artifact_url, buffer_d, store_artifact, file_path):
    """Buffer the artifact."""
    make_buffer(config, client_id, artifact_url, buffer_d, store_artifact, file_path)


@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--left_feature', required=True, help="URL of the first artifact.")
@click.option('--right_feature', required=True, help="URL of the second artifact.")
@click.option('--store-artifact', help="Store the intersected artifact.Set it to local/minio.")
@click.option('--file-path', help="Path to save the intersected artifact.")
def create_intersection(config, client_id, left_feature, right_feature, store_artifact, file_path):
    """Intersect the artifacts."""
    make_intersection(config, client_id, left_feature, right_feature, store_artifact, file_path)


@click.command()
@click.option('--location', required=True, help="Name of the place. City, State, Country etc.")
def list_data(location):
    """List data for a location."""
    data = list_features(location)
    click.echo(data)

@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact to download.")
@click.option('--store-artifact', help="Store the compute geomtery artifact. Set it to local/minio.")
@click.option('--file-path', help="Path to save the compute geomtery artifact.")
def compute_geometry(config, client_id, artifact_url, store_artifact, file_path):
    """
    Reads geospatial data from MinIO, computes geometry measures, and optionally saves the processed data back to MinIO.
    """
    compute_geometry_measures(config, client_id, artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--artifact-url', required=True, help="URL of the artifact.")
@click.option('--attribute', required=True, help="Attribute to reduce.")
@click.option('--grid-size', required=True, type=float, help="Size of the grid.")
@click.option('--reducer', required=True, help="Reducer operation.")
@click.option('--store-artifact', help="Store reduced raster. Set it to local/minio.")
@click.option('--file-path', help="Path to save the reduced raster artifact.")

def reduce_to_img(config, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path):
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    reduce_to_image(config, client_id, artifact_url, attribute, grid_size, reducer, store_artifact, file_path)


@click.command(name="create-optimal-route")
@click.option('--config', required=True, help="Path to MinIO config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--artifact-url', required=True, help="URL to road network object name in MinIO.")
@click.option('--points-filepath', required=True, help="Local path to the sample points (GeoJSON/Shapefile...).")
@click.option('--store-artifact', help="Store final route & points. Set it to local/minio.")
@click.option('--file-path', help="MinIO object name to store final route (GeoJSON).")
def create_optimal_route(config, client_id, artifact_url, points_filepath, store_artifact, file_path):
    """
    Compute a TSP-based optimal route by:
      - downloading a pickled road network from MinIO,
      - reading points from local disk,
      - building & solving TSP,
      - optionally storing route & points back to MinIO.
    """
    compute_optimal_route(config, client_id, artifact_url, points_filepath, store_artifact, file_path)

@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--input-artifact-url', required=True, help="URL of the point artifact to generate voronoi polygon.")
@click.option('--extend-artifact-url', default=None, help="URL of the artifact to define extend of output.")
@click.option('--tolerance', type=float, default=0.0, help="Snap input vertices together if their distance is less than this value.")
@click.option('--edges', type=bool, default=False, help="If set to True, the diagram will return LineStrings instead of Polygons.")
@click.option('--store-artifact', help="Store the voronoi polygons. Set it to local/minio.")
@click.option('--file-path', default=None, help="Path to save the voronoi artifact.")
def create_voronoi(config, client_id, input_artifact_url, extend_artifact_url, tolerance, edges, store_artifact, file_path):
    """Create Voronoi diagram based on input artefact"""
    create_voronoi_diagram(config, client_id, input_artifact_url, extend_artifact_url, store_artifact, file_path, tolerance, edges)

@click.command(name="clip-vector")
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--target-artifact-url', required=True, help="URL of the target artifact.")
@click.option('--clip-artifact-url', required=True, help="URL of the clip artifact")
@click.option('--store-artifact',  help="Store the clipped artifact. Set it to local/minio")
@click.option('--file-path', default="None", help="Path to save the clipped artifact")
def clip_vector(config, client_id, target_artifact_url, clip_artifact_url, store_artifact, file_path):
    """
    Clip a target feature to the extent of the clip feature,
    """
    make_clip(config, client_id, target_artifact_url, clip_artifact_url, store_artifact, file_path)

@click.command()
@click.option('--config', required=True, help="Path to minio config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="URL of the input artifact.")
@click.option('--store-artifact', help="Store delaunay artifact. Set it to local/minio")
@click.option('--file-path', default="None", help="Path to save the delaunay artifact")
@click.option('--qhull-options', default="", help="Additional Qhull options for scipy.spatial.Delaunay.")
def create_delaunay_triangles(config, client_id, artifact_url, store_artifact, file_path, qhull_options):
    """
    Create Delaunay triangles from a pickled GeoDataFrame/GeoSeries stored in MinIO,
    optionally uploading the result back to MinIO.
    """
    extra_kwargs = {"qhull_options": qhull_options} if qhull_options else {}
    make_delaunay_triangles(config, client_id, artifact_url, store_artifact, file_path,**extra_kwargs)


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
@click.option('--config', required=True, help="Path to the config file.")
def get_stac_assets(client_id, client_secret, role, collection_ids, config):
    '''Download Cartosat images from the STAC browser and stream to minio'''
    get_assets(client_id, client_secret, role, collection_ids, config)

@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name of the input DEM.")
@click.option('--threshold', required=True, type=float, help="threshold elevation upto which it is inundated.")
@click.option('--store-artifact', type=bool, default=False, help="Set to True to store the artifact in MinIO.")
@click.option('--file-path', default="", help="MinIO key name for flood fill layer. If not provided, a UUID name is used.")
def flood_fill_model(config, client_id, artifact_url, threshold, store_artifact, file_path):
    '''Create flood inundated raster based on input DEM and threshold value'''
    flood_fill(config, client_id, artifact_url, threshold, store_artifact, file_path)

@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="MinIO bucket name (client ID).")
@click.option('--artifact-url', required=True, help="MinIO object name of the input raster.")
@click.option('--interval', required=True, type=float, help="Specify intervals based on which defines the number of levels for isometric lines generated.")
@click.option('--store-artifact', type=bool, default=False, help="Set to True to store the artifact in MinIO.")
@click.option('--file-path', default="", help="MinIO key name for saving isometric lines generated. If not provided, a UUID name is used.")

def generate_isometric_lines(config, client_id, artifact_url, interval, store_artifact, file_path):
    '''Create flood inundated raster based on input DEM and threshold value'''
    isometric_lines(config, client_id, artifact_url, interval, store_artifact, file_path)


