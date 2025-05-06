import os
import io
import tempfile
from contextlib import redirect_stdout
from osgeo import gdal, ogr, osr

from common.minio_ops import connect_minio
from common.save_raster_artifact import save_raster_artifact
from common.convert_to_cog import tiff_to_cogtiff


def get_crs(filepath: str) -> str:
    """Get CRS WKT string from a raster or vector file."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext in ['.tif', '.tiff']:
        ds = gdal.Open(filepath)
        proj = ds.GetProjection()
        ds = None
    else:
        ds = ogr.Open(filepath)
        layer = ds.GetLayer()
        proj = layer.GetSpatialRef().ExportToWkt()
        ds = None
    return proj


def reproject_raster(input_raster: str, output_raster: str, target_srs_wkt: str):
    """Reproject raster to match target CRS."""
    warp_options = gdal.WarpOptions(
        dstSRS=target_srs_wkt,
        format="GTiff",
        resampleAlg='near',
        multithread=True,
        warpMemoryLimit=512,
    )
    gdal.Warp(
        destNameOrDestDS=output_raster,
        srcDSOrSrcDSTab=input_raster,
        options=warp_options
    )


def bbox_clip_raster(
    config_path: str,
    client_id: str,
    raster_key: str,
    vector_path: str,
    store_artifact: str = "minio",
    file_path: str | None = None,
) -> str:
    """
    Clip a raster with a bbox input as polygon GeoJSON (provided locally), producing one Cloud-Optimized GeoTIFF. In editor it will be renamed as bbox-raster-clip.

    Parameters
    ----------
    config_path : str (Reactflow will ignore this parameter)
    client_id   : str (Reactflow will translate it as input)
    raster_key  : str (Reactflow will take it from the previous step)
    vector_path : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path   : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config_path, client_id)

    with tempfile.TemporaryDirectory() as tmpdir:
        loc_rst   = os.path.join(tmpdir, "input_raster.tif")
        aligned_rst = os.path.join(tmpdir, "aligned_raster.tif")
        raw_clip  = os.path.join(tmpdir, "raw_clip.tif")
        final_cog = os.path.join(tmpdir, "clip_cog.tif")

        # Download raster from MinIO
        client.fget_object(client_id, raster_key, loc_rst)

        # Step 1: Check CRS
        raster_crs = get_crs(loc_rst)
        vector_crs = get_crs(vector_path)

        if raster_crs != vector_crs:
            reproject_raster(loc_rst, aligned_rst, target_srs_wkt=vector_crs)
            raster_to_use = aligned_rst
        else:
            raster_to_use = loc_rst

        # Step 2: Clip raster using vector
        warp_options = gdal.WarpOptions(
            format="GTiff",
            cutlineDSName=vector_path,
            cropToCutline=True,
            dstNodata=0,
            resampleAlg='near',
            outputType=gdal.GDT_Float32,
            multithread=True,
            warpMemoryLimit=512,
            cutlineBlend=0,
        )

        result = gdal.Warp(
            destNameOrDestDS=raw_clip,
            srcDSOrSrcDSTab=raster_to_use,
            options=warp_options
        )

        if result is None:
            raise RuntimeError("[ERROR] GDAL Warp failed during clipping.")

        result.FlushCache()
        result = None

        # Step 3: Convert clipped raster to Cloud Optimized GeoTIFF
        tiff_to_cogtiff(raw_clip, final_cog)

        # Step 4: Save to MinIO or locally
        with io.StringIO() as _buf, redirect_stdout(_buf):
            save_raster_artifact(
                config=config_path,
                client_id=client_id,
                local_path=final_cog,
                file_path=file_path,
                store_artifact=store_artifact,
            )
        print(f"{file_path}")
        return file_path if store_artifact.lower() == "minio" else final_cog
