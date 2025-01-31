import gdi_cli
import click


@click.group()
def cli():
    """CLI tool for generating tokens and fetching resources."""
    
    pass

cli.add_command(gdi_cli.generate_token)
cli.add_command(gdi_cli.fetch_resource)
cli.add_command(gdi_cli.features_count)
cli.add_command(gdi_cli.ls_objects)
cli.add_command(gdi_cli.download_artifact)
cli.add_command(gdi_cli.create_buffer)