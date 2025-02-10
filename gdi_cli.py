import click
from auth.token_gen import TokenGenerator
from features.get_re import ResourceFetcher
from features.count_features import count_features
from features.download_features import download_features
from features.buffer import make_buffer
from features.intersection import make_intersection
from features.gcode import list_features
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
@click.option('--save-as',required = True , help="Save the fetched object to Minio as the given file name.")
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