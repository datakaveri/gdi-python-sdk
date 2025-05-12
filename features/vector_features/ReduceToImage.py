import geopandas as gpd
import rasterio
import numpy as np
from rasterio.transform import from_origin
import io
import os
import pandas as pd
from shapely.geometry import box
from osgeo import gdal, gdalconst

from common.minio_ops import connect_minio
from common.save_raster_artifact import save_raster_artifact

def create_grid(gdf, grid_size) -> gpd.GeoDataFrame:
    if gdf.crs.to_epsg() == 4326:
        gdf = gdf.to_crs(epsg=7755)

    bounds = gdf.total_bounds
    minx, miny, maxx, maxy = bounds
    x_coords = np.arange(minx, maxx, grid_size)
    y_coords = np.arange(miny, maxy, grid_size)

    grid_cells = [box(x, y, x + grid_size, y + grid_size) for x in x_coords for y in y_coords]
    return gpd.GeoDataFrame({'geometry': grid_cells}, crs=gdf.crs)

def apply_reducer(grid, vector_data, attribute, reducer) -> gpd.GeoDataFrame:
    if attribute not in vector_data.columns:
        raise ValueError(f"Attribute '{attribute}' not found in vector data.")

    attribute_dtype = vector_data[attribute].dtype
    is_numeric = np.issubdtype(attribute_dtype, np.number)

    if reducer in ["min", "max", "sum", "mean"] and not is_numeric:
        raise ValueError(f"Reducer '{reducer}' can only be applied to numeric attributes.")

    grid["raster_val"] = None
    if reducer in ["first", "last", "mode", "concat"]:
        grid["description"] = None

    category_mapping = {}
    category_counter = 1

    for idx, cell in grid.iterrows():
        intersecting = vector_data[vector_data.intersects(cell.geometry)]
        if not intersecting.empty:
            values = intersecting[attribute]
            val = None
            if reducer == "count":
                val = len(values)
            elif reducer == "density":
                area = cell.geometry.area / 1e6
                val = len(values) / area if area > 0 else 0
            elif reducer == "sum":
                val = values.sum()
            elif reducer == "mean":
                val = values.mean()
            elif reducer == "min":
                val = values.min()
            elif reducer == "max":
                val = values.max()
            elif reducer in ["mode", "first", "last", "concat"]:
                if reducer == "mode":
                    mode_vals = values.mode()
                    val_str = ", ".join(map(str, mode_vals)) if not mode_vals.empty else None
                elif reducer == "first":
                    val_str = values.iloc[0]
                elif reducer == "last":
                    val_str = values.iloc[-1]
                elif reducer == "concat":
                    val_str = ", ".join(values.astype(str))

                if val_str not in category_mapping:
                    category_mapping[val_str] = category_counter
                    category_counter += 1
                val = category_mapping[val_str]
                grid.at[idx, "description"] = val_str

            grid.at[idx, "raster_val"] = val

    return grid

def convert_to_raster(grid, grid_size, output_raster) -> str:
    bounds = grid.total_bounds
    minx, miny, maxx, maxy = bounds

    cols = int((maxx - minx) / grid_size)
    rows = int((maxy - miny) / grid_size)
    transform = from_origin(minx, maxy, grid_size, grid_size)

    raster_data = np.full((rows, cols), np.nan)
    category_descriptions = {}

    for _, row in grid.iterrows():
        centroid = row.geometry.centroid
        col = int((centroid.x - minx) / grid_size)
        row_idx = int((maxy - centroid.y) / grid_size)
        value = row["raster_val"]

        if pd.notna(value):
            raster_data[row_idx, col] = int(value)
            if "description" in grid.columns and pd.notna(row.get("description")):
                category_descriptions[int(value)] = row["description"]

    metadata = {
        'driver': 'GTiff',
        'height': rows,
        'width': cols,
        'count': 1,
        'dtype': rasterio.uint8,
        'crs': grid.crs,
        'transform': transform
    }

    with rasterio.open(output_raster, 'w', **metadata) as dst:
        dst.write(raster_data, 1)
        dst.update_tags(**{"Category_Descriptions": str(category_descriptions)})

    _add_rat(output_raster, category_descriptions)
    return output_raster

def _add_rat(file_path, category_descriptions):
    ds = gdal.Open(file_path, gdalconst.GA_Update)
    band = ds.GetRasterBand(1)
    rat = gdal.RasterAttributeTable()
    rat.CreateColumn('Value', gdalconst.GFT_Integer, gdalconst.GFU_Generic)
    rat.CreateColumn('Class', gdalconst.GFT_String, gdalconst.GFU_Name)

    for idx, (value, label) in enumerate(category_descriptions.items()):
        rat.SetValueAsInt(idx, 0, value)
        rat.SetValueAsString(idx, 1, label)

    band.SetDefaultRAT(rat)
    band.SetMetadata({'LAYER_TYPE': 'thematic'})
    ds = None

def reduce_to_image(config: str, client_id: str, artifact_url: str, attribute: str, grid_size: float, reducer: str, store_artifact: str, file_path: str = None) -> None:
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.In editor it will be renamed as reduce-to-raster.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    attribute : str (Reactflow will translate it as input)
    grid_size : int (Reactflow will translate it as input)
    reducer : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)

    try:
        with client.get_object(client_id, artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        if gdf.crs.to_epsg() != 7755:
            gdf = gdf.to_crs(epsg=7755)

        grid = create_grid(gdf, grid_size)
        grid = apply_reducer(grid, gdf, attribute, reducer)

        temp_raster = "temp_output.tif"
        cog_path = convert_to_raster(grid, grid_size, temp_raster)

        # Save COG using external utility
        if store_artifact:
            save_raster_artifact(config=config, client_id=client_id, local_path=cog_path, file_path=file_path, store_artifact=store_artifact)

        else:
            print("Data not saved. Set store_artifact to minio/local to save the data to minio or locally.")
            print("Feature reduced to raster successfully")

        # Clean up
        for f in [temp_raster, cog_path, f"{temp_raster}.aux.xml"]:
            if os.path.exists(f):
                os.remove(f)

    except Exception as e:
        raise RuntimeError(f"Error during raster reduction: {str(e)}")
