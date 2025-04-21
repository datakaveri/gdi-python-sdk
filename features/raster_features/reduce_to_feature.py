import os
import io
import tempfile
import numpy as np
import geopandas as gpd
import warnings
from osgeo import gdal, ogr, osr
from common.minio_ops import connect_minio
from common.save_raster_artifact import save_raster_artifact

warnings.filterwarnings("ignore")

def extract_raster_to_vector(
    config: str,
    client_id: str,
    raster_artifact_url: str,
    vector_artifact_url: str,
    reducer: str,
    sttribute: str,
    store_artifact: str,
    file_path: str = None
) -> str:
    """
    Extract raster values to vector features using polygonized raster and spatial join with reducer. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as flood-fill-model.
    Parameters
    ----------
    config : str (Reactflow will translate it as input)
    client_id : str (Reactflow will translate it as input)
    raster_artifact_url : str (Reactflow will take it from the previous step)
    vector_artifact_url : str (Reactflow will take it from the previous step)
    reducer : str (Reactflow will translate it as input)
    sttribute : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)
    temp_dir = tempfile.mkdtemp()
    temp_raster_path = os.path.join(temp_dir, "temp_raster.tif")
    temp_polygon_path = os.path.join(temp_dir, "temp_raster_polygon.geojson")
    output_path = os.path.join(temp_dir, "output_vector.geojson")

    try:
        # Download raster from MinIO
        with client.get_object(client_id, raster_artifact_url) as response:
            raster_data = response.read()
        with open(temp_raster_path, "wb") as f:
            f.write(raster_data)

        # Polygonize raster
        src_ds = gdal.Open(temp_raster_path)
        srcband = src_ds.GetRasterBand(1)
        nodata = srcband.GetNoDataValue()

        drv = ogr.GetDriverByName("GeoJSON")
        if os.path.exists(temp_polygon_path):
            drv.DeleteDataSource(temp_polygon_path)
        out_ds = drv.CreateDataSource(temp_polygon_path)
        srs = osr.SpatialReference()
        srs.ImportFromWkt(src_ds.GetProjection())
        out_layer = out_ds.CreateLayer("polygonized", srs=srs)
        field_defn = ogr.FieldDefn("value", ogr.OFTReal)
        out_layer.CreateField(field_defn)
        gdal.Polygonize(srcband, None, out_layer, 0, [], callback=None)
        if nodata is not None:
            out_layer.SetAttributeFilter(f"value != {nodata}")
        src_ds, out_ds = None, None

        # Read vector from MinIO using BytesIO
        with client.get_object(client_id, vector_artifact_url) as response:
            vec_gdf = gpd.read_file(io.BytesIO(response.read()))

        raster_poly_gdf = gpd.read_file(temp_polygon_path)

        if vec_gdf.crs != raster_poly_gdf.crs:
            raster_poly_gdf = raster_poly_gdf.to_crs(vec_gdf.crs)

        if vec_gdf.geom_type.str.contains("Multi").any():
            vec_gdf = vec_gdf.explode(index_parts=False)

        joined = gpd.overlay(vec_gdf, raster_poly_gdf, how='intersection')
        agg = joined.groupby(joined.index)[["value"]]

        if reducer == "mean":
            stats = agg.mean()
        elif reducer == "min":
            stats = agg.min()
        elif reducer == "max":
            stats = agg.max()
        elif reducer == "count":
            stats = agg.count()
        elif reducer == "sum":
            stats = agg.sum()
        else:
            raise ValueError(f"Unsupported reducer: {reducer}")

        vec_gdf[sttribute] = vec_gdf.index.map(stats["value"].get)
        vec_gdf.to_file(output_path, driver="GeoJSON")

        if store_artifact:
            save_raster_artifact(
                config=config,
                client_id=client_id,
                local_path=output_path,
                file_path=file_path,
                store_artifact=store_artifact
            )
        else:
            print("Data not saved. Set store_artifact to minio/local to save the data.")
            print("Flood fill computed successfully.")

    except Exception as e:
        raise RuntimeError(f"[ERROR] Raster to vector extraction failed: {e}")

    finally:
        try:
            for f in [temp_raster_path, temp_polygon_path, output_path]:
                if os.path.exists(f):
                    os.remove(f)
            os.rmdir(temp_dir)
        except Exception:
            pass
