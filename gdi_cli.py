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


from features.raster_features.search_cat import search_stac
from features.raster_features.get_data import get_assets

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
    client_id : str (Node red will translate it as input)
    client_secret : str (Node red will translate it as input)
    role : enum [consumer, provider, admin] (Node red will translate it as input)
    resource_id : str (Node red will translate it as input)
    save_object : enum [True, False] (Node red will translate it as input)
    config_path : str (Node red will translate it as input)
    file_path : str (Node red will ignore this parameter)

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
@click.option('--save-object',  help="Save the fetched object to Minio. set it to True or False.")
@click.option('--config-path',  help="Path to the Minio configuration file. Olny if you are saving the object.")
@click.option('--file-path', help="Path to save the fetched object.")


def fetch_resource(client_id, client_secret, role, resource_id, save_object, config_path, file_path):
    """Fetch resource data."""
    resource_data = get_resource(client_id, client_secret, role, resource_id, save_object, config_path, file_path)
    



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
@click.option('--store-artifact', help="Store the buffered artifact.")
@click.option('--file-path', help="Path to save the buffered artifact.")
def create_buffer(config, client_id, artifact_url, buffer_d, store_artifact, file_path):
    """Buffer the artifact."""
    make_buffer(config, client_id, artifact_url, buffer_d, store_artifact, file_path)


@click.command()
@click.option('--config', required=True, help="Path to the config file.")
@click.option('--client-id', required=True, help="Client ID for authentication.")
@click.option('--left_feature', required=True, help="URL of the first artifact.")
@click.option('--right_feature', required=True, help="URL of the second artifact.")
@click.option('--store-artifact', help="Store the intersected artifact.")
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
@click.option('--store-artifact', help="Store the intersected artifact.")
@click.option('--file-path', help="Path to save the intersected artifact.")
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
@click.option('--grid-size', required=True, type=int, help="Size of the grid.")
@click.option('--reducer', required=True, help="Reducer operation.")
@click.option('--store-artefacts', help="Store the intersected artifact.")
@click.option('--file-path', help="Path to save the intersected artifact.")

def reduce_to_img(config, client_id, artifact_url, attribute, grid_size, reducer, store_artefacts, file_path):
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    reduce_to_image(config, client_id, artifact_url, attribute, grid_size, reducer, store_artefacts, file_path)


@click.command(name="create-optimal-route")
@click.option('--config', required=True, help="Path to MinIO config file.")
@click.option('--client-id', required=True, help="MinIO bucket name.")
@click.option('--artifact-url', required=True, help="Pickled road network object name in MinIO.")
@click.option('--points-filepath', required=True, help="Local path to the sample points (GeoJSON/Shapefile...).")
@click.option('--store-artifacts', help="Set to upload final route & points to MinIO.")
@click.option('--route-file-path', help="MinIO object name to store final route (GeoJSON).")
def create_optimal_route(config, client_id, artifact_url, points_filepath, store_artifacts, route_file_path):
    """
    Compute a TSP-based optimal route by:
      - downloading a pickled road network from MinIO,
      - reading points from local disk,
      - building & solving TSP,
      - optionally storing route & points back to MinIO.
    """
    compute_optimal_route(config, client_id, artifact_url, points_filepath, store_artifacts, route_file_path)


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






