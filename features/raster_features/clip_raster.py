import os
import tempfile
import io
from contextlib import redirect_stdout
from osgeo import gdal, ogr, osr

from common.minio_ops import connect_minio
from common.save_raster_artifact import save_raster_artifact
from common.convert_to_cog import tiff_to_cogtiff


def clip_raster(
    config_path: str,
    client_id: str,
    raster_key: str,
    geojson_key: str,
    store_artifact: str = "minio",
    file_path: str | None = None,
) -> str:
    """
    Clip a raster with a polygon GeoJSON, producing one Cloud-Optimized GeoTIFF.

    Parameters
    ----------
    config_path : str (Reactflow will ignore this parameter)
    client_id   : str (Reactflow will translate it as input)
    raster_key  : str (Reactflow will take it from the previous step)
    geojson_key : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path   : str (Reactflow will ignore this parameter)
    """
    client = connect_minio(config_path, client_id)

    with tempfile.TemporaryDirectory() as tmp:
        loc_geo   = os.path.join(tmp, "poly.geojson")
        loc_rst   = os.path.join(tmp, "src.tif")
        raw_clip  = os.path.join(tmp, "clip_raw.tif")
        final_cog = os.path.join(tmp, "clip_cog.tif")

        client.fget_object(client_id, geojson_key, loc_geo)
        client.fget_object(client_id, raster_key,  loc_rst)

        drv = ogr.GetDriverByName("GeoJSON")
        ds  = drv.Open(loc_geo, 0)
        layer    = ds.GetLayer()
        union    = None
        for ft in layer:
            g = ft.GetGeometryRef()
            union = g.Clone() if union is None else union.Union(g)
        poly_srs = layer.GetSpatialRef()
        ds = None

        with gdal.Open(loc_rst) as rst_ds:
            rst_srs = osr.SpatialReference(wkt=rst_ds.GetProjection())

        if not poly_srs.IsSame(rst_srs):
            union.Transform(osr.CoordinateTransformation(poly_srs, rst_srs))

        vsicut = "/vsimem/clip.geojson"
        gdal.Unlink(vsicut)
        ds   = drv.CreateDataSource(vsicut)
        lyr  = ds.CreateLayer("clip", srs=rst_srs, geom_type=ogr.wkbPolygon)
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetGeometry(union)
        lyr.CreateFeature(feat)
        ds = None

        gdal.Warp(
            raw_clip, loc_rst,
            format="GTiff",
            cutlineDSName=vsicut,
            cropToCutline=True,
            dstNodata=0,
            callback=None,
        )

        tiff_to_cogtiff(raw_clip, final_cog)

        with io.StringIO() as _buf, redirect_stdout(_buf):
            save_raster_artifact(
                config=config_path,
                client_id=client_id,
                local_path=final_cog,
                file_path=file_path,
                store_artifact=store_artifact,
            )

        return file_path if store_artifact.lower() == "minio" else final_cog
