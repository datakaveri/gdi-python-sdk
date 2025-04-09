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
cli.add_command(gdi_cli.create_intersection)
cli.add_command(gdi_cli.list_data)
cli.add_command(gdi_cli.compute_geometry)
cli.add_command(gdi_cli.reduce_to_img)
cli.add_command(gdi_cli.get_stac_assets)
cli.add_command(gdi_cli.create_optimal_route)
cli.add_command(gdi_cli.create_voronoi)
cli.add_command(gdi_cli.clip_vector)
cli.add_command(gdi_cli.create_delaunay_triangles)
cli.add_command(gdi_cli.flood_fill_model)
cli.add_command(gdi_cli.generate_isometric_lines)
cli.add_command(gdi_cli.generate_slope)
cli.add_command(gdi_cli.generate_ndvi)
