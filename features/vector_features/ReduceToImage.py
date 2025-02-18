import geopandas as gpd
import rasterio
import numpy as np
from rasterio.transform import from_origin
from shapely.geometry import box
from common.minio_ops import connect_minio
import pickle as pkl
import os
import uuid

def create_grid(gdf, grid_size) -> gpd.GeoDataFrame:
    """
    Creates a grid over the given geospatial data. If the input CRS is EPSG:4326, 
    it reprojects it to EPSG:7755 to ensure that the grid size is in meters.
    """
    
    bounds = gdf.total_bounds
    minx, miny, maxx, maxy = bounds
    
    x_coords = np.arange(minx, maxx, grid_size)
    y_coords = np.arange(miny, maxy, grid_size)
    
    grid_cells = []
    for x in x_coords:
        for y in y_coords:
            grid_cells.append(box(x, y, x + grid_size, y + grid_size))
    
    grid = gpd.GeoDataFrame({'geometry': grid_cells}, crs=gdf.crs)
    return grid

def apply_reducer(grid, vector_data, attribute, reducer) -> gpd.GeoDataFrame:
    """
    Applies a reducer function to the given attribute within grid cells.
    """
    if attribute not in vector_data.columns:
        raise ValueError(f"Attribute '{attribute}' not found in vector data.")
    
    attribute_dtype = vector_data[attribute].dtype
    is_numeric = np.issubdtype(attribute_dtype, np.number)
    
    if reducer not in ["count", "density"] and not is_numeric:
        raise ValueError(f"Reducer '{reducer}' can only be applied to numeric attributes. '{attribute}' is not numeric.")
    
    grid["raster_val"] = None
    
    for idx, cell in grid.iterrows():
        intersecting = vector_data[vector_data.intersects(cell.geometry)]
        if not intersecting.empty:
            values = intersecting[attribute]
            
            if reducer == "count":
                grid.at[idx, "raster_val"] = len(values)
            elif reducer == "density":
                grid.at[idx, "raster_val"] = len(values) / cell.geometry.area
            elif reducer == "sum":
                grid.at[idx, "raster_val"] = values.sum()
            elif reducer == "mean":
                grid.at[idx, "raster_val"] = values.mean()
            elif reducer == "min":
                grid.at[idx, "raster_val"] = values.min()
            elif reducer == "max":
                grid.at[idx, "raster_val"] = values.max()
    
    return grid

def convert_to_raster(grid, output_raster, grid_size) -> rasterio.io.DatasetWriter:
    """
    Converts processed grid data into a raster image and saves it to MinIO.
    """
    bounds = grid.total_bounds
    minx, miny, maxx, maxy = bounds
    
    cols = int((maxx - minx) / grid_size)
    rows = int((maxy - miny) / grid_size)
    
    transform = from_origin(minx, maxy, grid_size, grid_size)
    raster_data = np.full((rows, cols), np.nan)
    
    for _, row in grid.iterrows():
        x, y = row.geometry.centroid.x, row.geometry.centroid.y
        col = int((x - minx) / grid_size)
        row_idx = int((maxy - y) / grid_size)
        raster_data[row_idx, col] = row["raster_val"]
    
    with rasterio.open(
        output_raster, 'w', driver='GTiff',
        height=rows, width=cols,
        count=1, dtype=raster_data.dtype,
        crs=grid.crs,
        transform=transform
    ) as dst:
        dst.write(raster_data, 1)
    
    # return dst

def reduce_to_image(config: str, client_id: str, artefact_url: str, attribute: str, grid_size: int, reducer: str, store_artefacts: bool = False, file_path: str = None) -> rasterio.io.DatasetWriter:
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    client = connect_minio(config, client_id)
    
    try:
        with client.get_object(client_id, artefact_url) as response:
            gdf = pkl.loads(response.read())

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        if gdf.crs.to_epsg() != 7755:
            gdf = gdf.to_crs(epsg=7755)
        
        grid = create_grid(gdf, grid_size)
        grid = apply_reducer(grid, gdf, attribute, reducer)
        
        output_raster = "temp.tif"
        convert_to_raster(grid, output_raster, grid_size)
        
        if store_artefacts:
            if not file_path:
                file_path = f"processed_rasters/{uuid.uuid4()}.tif"
            try:
                client.fput_object(client_id, file_path, output_raster)
                os.remove(output_raster)
                print(file_path)
            except Exception as e:
                raise Exception(f"Error while saving raster file: {e}")
        else:
            print("Raster not saved. Set store_artefacts to True to save the data to MinIO.")
            print("Raster processing completed.")
        
    except Exception as e:
        raise e
    
    

# reduce_to_image('config.json', 'c669d152-592d-4a1f-bc98-b5b73111368e', 'Census_Abstract_Varanasi_ce17126b-4972-4971-af44-c8c9de57a98a.pkl', 'NON_WORK_P', 1000, 'sum', True, 'processed_rasters/output_census_NON_WORK_P_sum.tif')
