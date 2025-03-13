import geopandas as gpd
import rasterio
import numpy as np
from rasterio.transform import from_origin
from rasterio.enums import Resampling
from shapely.geometry import box
from common.minio_ops import connect_minio
import pickle as pkl
import os
import uuid

import tempfile
import pandas as pd
from osgeo import gdal, gdalconst

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
    
    if reducer in ["min", "max", "sum", "mean"] and not is_numeric:
        raise ValueError(f"Reducer '{reducer}' can only be applied to numeric attributes.")
    
    if grid.crs.to_epsg() != 7755:
        grid = grid.to_crs(epsg=7755)
    
    grid["raster_val"] = None
    if reducer in ["first", "last", "mode", "concat"]:
        grid["description"] = None  
    
    category_mapping = {}
    category_counter = 1
    
    for idx, cell in grid.iterrows():
        intersecting = vector_data[vector_data.intersects(cell.geometry)]
        if not intersecting.empty:
            values = intersecting[attribute]
            
            if reducer == "count":
                grid.at[idx, "raster_val"] = len(values)
            elif reducer == "density":
                area_in_sq_km = cell.geometry.area / 1e6  
                grid.at[idx, "raster_val"] = len(values) / area_in_sq_km if area_in_sq_km > 0 else 0
            elif reducer == "sum":
                grid.at[idx, "raster_val"] = values.sum()
            elif reducer == "mean":
                grid.at[idx, "raster_val"] = values.mean()
            elif reducer == "min":
                grid.at[idx, "raster_val"] = values.min()
            elif reducer == "max":
                grid.at[idx, "raster_val"] = values.max()
            elif reducer in ["mode", "first", "last", "concat"]:
                if reducer == "mode":
                    mode_vals = values.mode()
                    mode_val = ", ".join(map(str, mode_vals)) if not mode_vals.empty else None
                elif reducer == "first":
                    mode_val = values.iloc[0]
                elif reducer == "last":
                    mode_val = values.iloc[-1]
                elif reducer == "concat":
                    mode_val = ", ".join(values.astype(str))
                
                if mode_val not in category_mapping:
                    category_mapping[mode_val] = category_counter
                    category_counter += 1
                grid.at[idx, "raster_val"] = category_mapping[mode_val]
                grid.at[idx, "description"] = mode_val  
    
    return grid

def convert_to_raster(grid, grid_size, output_raster) -> rasterio.io.DatasetWriter:
    """
    Converts processed grid data into a Cloud-Optimized GeoTIFF (COG) and saves it.
    """   
    bounds = grid.total_bounds
    minx, miny, maxx, maxy = bounds
    
    cols = int((maxx - minx) / grid_size)
    rows = int((maxy - miny) / grid_size)
    
    transform = from_origin(minx, maxy, grid_size, grid_size)
    raster_data = np.full((rows, cols), np.nan)  
    
    category_descriptions = {}
    
    for _, row in grid.iterrows():
        x, y = row.geometry.centroid.x, row.geometry.centroid.y
        col = int((x - minx) / grid_size)
        row_idx = int((maxy - y) / grid_size)
        raster_value = row["raster_val"]
        
        if pd.notna(raster_value):
            raster_data[row_idx, col] = int(raster_value)
            if "description" in grid.columns and pd.notna(row.get("description", None)):
                category_descriptions[int(raster_value)] = row["description"]
    
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
    
    add_rat(output_raster, category_descriptions)
   
    
    # Convert to COG (writes to a local path)
    output_cog = convert_to_cog(output_raster)
    
    return output_cog

def add_rat(file_path, category_descriptions):
    """
    Creates RAT (Raster Attribute Table) for the raster, if additional attributes are required to be added.
    """
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
    ds = None  # Close dataset

def convert_to_cog(input_tiff):
    """Convert a regular TIFF to Cloud Optimized GeoTIFF while preserving RAT"""
    output_cog = f"COG_{os.path.basename(input_tiff)}"
    
    try:
       

        # Use gdal_translate instead of cog_translate to preserve RAT
        translate_options = gdal.TranslateOptions(format="COG", metadataOptions=["COPY_SRC_OVERVIEWS=YES"])
        gdal.Translate(output_cog, input_tiff, options=translate_options)

        

        # Check if RAT is present in COG
        ds = gdal.Open(output_cog, gdalconst.GA_ReadOnly)
        band = ds.GetRasterBand(1)
        rat = band.GetDefaultRAT()
      

        return output_cog

    except Exception as e:
        print(f"Error converting to COG: {str(e)}")
        return None

# reduce_to_image(config='config.json', client_id='c669d152-592d-4a1f-bc98-b5b73111368e', artifact_url='School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.pkl', attribute='schname', grid_size=1000, reducer='mode', store_artifacts=True, file_path='raster_reducers/cog_school_schname_mode.tif')  

def reduce_to_image(config: str, client_id: str, artifact_url: str, attribute: str, grid_size: int, reducer: str, store_artifacts: bool = False, file_path: str = None) -> rasterio.io.DatasetWriter:
    """
    Reads vector data from MinIO, applies reduction operation, and stores the output raster in MinIO.
    """
    client = connect_minio(config, client_id)
    
    try:
        with client.get_object(client_id, artifact_url) as response:
            gdf = pkl.loads(response.read())

        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        if gdf.crs.to_epsg() != 7755:
            gdf = gdf.to_crs(epsg=7755)

        grid = create_grid(gdf, grid_size)
        grid = apply_reducer(grid, gdf, attribute, reducer)
        output_raster = "temp.tif"
        output_cog = convert_to_raster(grid, grid_size, output_raster)
        
                      
        if store_artifacts:
            if not file_path:
                file_path = f"processed_rasters/{uuid.uuid4()}.tif"
            try:
              
                client.fput_object(client_id, file_path, output_cog)
                #also add xml file to have raster attributes
                xmlpath = file_path+".aux.xml"
                xmlfile = output_cog+".aux.xml"
                
                #add condition if xmlfile exists
                if os.path.exists(xmlfile):
                    client.fput_object(client_id, xmlpath, xmlfile)           
                os.remove(output_cog)
                os.remove(xmlfile)
                os.remove(output_raster)
                os.remove('temp.tif.aux.xml')
                print(file_path)
            except Exception as e:
                raise Exception(f"Error while saving raster file: {e}")
        else:
            print("Raster not saved. Set store_artefacts to True to save the data to MinIO.")
            print("Raster processing completed.")
        
    except Exception as e:
        raise e

# reduce_to_image(config='config.json', client_id='c669d152-592d-4a1f-bc98-b5b73111368e', artifact_url='School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.pkl', attribute='schname', grid_size=1000, reducer='mode', store_artifacts=True, file_path='raster_reducers/cog_school_schname_mode.tif')    

# reduce_to_image('config.json', '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', 'intermediate/School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.pkl', 'schname', 1000, 'mode', True, 'processed_rasters/School_Varanasi_81537895-3da1-4dcd-af6f-053bc07afcf9.tif')
