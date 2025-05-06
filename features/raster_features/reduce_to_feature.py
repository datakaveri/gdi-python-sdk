import os
import io
import tempfile
import numpy as np
import geopandas as gpd
import warnings
from osgeo import gdal, ogr, osr
from shapely.geometry import mapping
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature

warnings.filterwarnings("ignore")

def extract_raster_to_vector(
    config: str,
    client_id: str,
    raster_artifact_url: str,
    vector_artifact_url: str,
    reducer: str,
    attribute: str,
    store_artifact: str,
    file_path: str = None
) -> str:
    """
    Extract raster values to vector features using polygonized raster and spatial join with reducer. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as reduce-to-feature.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    raster_artifact_url : str (Reactflow will take it from the previous step)
    vector_artifact_url : str (Reactflow will take it from the previous step)
    reducer : str (Reactflow will translate it as input)
    attribute : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)
    temp_dir = tempfile.mkdtemp()
    temp_raster_path = os.path.join(temp_dir, "input_raster.tif")

    try:
        # --- Step 1: Download raster ---
        with client.get_object(client_id, raster_artifact_url) as response:
            with open(temp_raster_path, "wb") as f:
                f.write(response.read())

        # --- Step 2: Read vector data ---
        with client.get_object(client_id, vector_artifact_url) as response:
            vec_gdf = gpd.read_file(io.BytesIO(response.read()))

        # ---  Explode MultiPolygons ---
        vec_gdf = vec_gdf.explode(index_parts=False).reset_index(drop=True)

        # --- Step 3: Open raster ---
        raster_ds = gdal.Open(temp_raster_path)
        band = raster_ds.GetRasterBand(1)
        nodata = band.GetNoDataValue()
        geotransform = raster_ds.GetGeoTransform()
        inv_gt = gdal.InvGeoTransform(geotransform)
        arr = band.ReadAsArray().astype(np.float64)
        arr[arr == nodata] = np.nan

        # --- Step 4: Sample raster values per vector feature ---
        results = []
        wkt = raster_ds.GetProjection()
        srs = osr.SpatialReference()
        srs.ImportFromWkt(wkt)

        for geom in vec_gdf.geometry:
            mask_ds = gdal.GetDriverByName('MEM').Create('', raster_ds.RasterXSize, raster_ds.RasterYSize, 1, gdal.GDT_Byte)
            mask_ds.SetGeoTransform(geotransform)
            mask_ds.SetProjection(raster_ds.GetProjection())

            # Create in-memory layer
            mem_drv = ogr.GetDriverByName('Memory')
            mem_ds = mem_drv.CreateDataSource('out')
            mem_layer = mem_ds.CreateLayer('layer', srs=srs, geom_type=ogr.wkbPolygon)
            feature_def = mem_layer.GetLayerDefn()

            # Convert Shapely geometry to OGR
            ogr_geom = ogr.CreateGeometryFromWkb(geom.wkb)
            feature = ogr.Feature(feature_def)
            feature.SetGeometry(ogr_geom)
            mem_layer.CreateFeature(feature)

            # Rasterize the single-feature layer
            gdal.RasterizeLayer(mask_ds, [1], mem_layer, burn_values=[1])

            # Cleanup
            feature = None
            mem_ds = None

            mask_arr = mask_ds.ReadAsArray().astype(bool)
            masked_values = arr[mask_arr]

            if masked_values.size == 0:
                results.append(np.nan)
            else:
                if reducer == "mean":
                    results.append(np.nanmean(masked_values))
                elif reducer == "min":
                    results.append(np.nanmin(masked_values))
                elif reducer == "max":
                    results.append(np.nanmax(masked_values))
                elif reducer == "sum":
                    results.append(np.nansum(masked_values))
                elif reducer == "count":
                    results.append(np.sum(~np.isnan(masked_values)))
                else:
                    raise ValueError(f"Unsupported reducer: {reducer}")

        # --- Step 5: Assign to new column ---
        vec_gdf[attribute] = results

        # --- Step 6: Save ---
        if store_artifact:
            save_feature(
                client_id=client_id,
                store_artifact=store_artifact,
                gdf=vec_gdf,
                file_path=file_path,
                config_path=config
            )
        else:
            print("Output not saved. Set `store_artifact` to 'minio' or 'local' to save.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] Failed during raster-to-vector extraction: {e}")

    finally:
        try:
            os.remove(temp_raster_path)
            os.rmdir(temp_dir)
        except Exception:
            pass
