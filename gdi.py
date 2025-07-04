import gdi_cli
import click


@click.group()
def cli():
    """CLI tool for generating tokens and fetching resources."""
    
    pass

cli.add_command(gdi_cli.generate_token)
cli.add_command(gdi_cli.get_vector_data)
cli.add_command(gdi_cli.features_count)
cli.add_command(gdi_cli.ls_objects)
cli.add_command(gdi_cli.download_vector_features)
cli.add_command(gdi_cli.create_buffer)
cli.add_command(gdi_cli.create_intersection)
cli.add_command(gdi_cli.list_data)
cli.add_command(gdi_cli.compute_geometry)
cli.add_command(gdi_cli.reduce_to_raster)
cli.add_command(gdi_cli.get_raster_data)
cli.add_command(gdi_cli.create_optimal_route)
cli.add_command(gdi_cli.create_voronoi)
cli.add_command(gdi_cli.clip_vector)
cli.add_command(gdi_cli.create_delaunay_triangles)
cli.add_command(gdi_cli.flood_fill_model)
cli.add_command(gdi_cli.generate_isometric_lines)
cli.add_command(gdi_cli.generate_slope)
cli.add_command(gdi_cli.generate_ndvi)
cli.add_command(gdi_cli.raster_clip)
cli.add_command(gdi_cli.rasters_merge)
cli.add_command(gdi_cli.download_raster)
cli.add_command(gdi_cli.bbox_raster_clip)
cli.add_command(gdi_cli.bbox_feature_clip)
cli.add_command(gdi_cli.generate_local_correlation)
cli.add_command(gdi_cli.reduce_to_feature)
cli.add_command(gdi_cli.extract_band_path)
cli.add_command(gdi_cli.convert_vector)