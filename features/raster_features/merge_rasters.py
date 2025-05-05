import os
import tempfile
from contextlib import redirect_stdout
import io
from typing import List, Dict

from osgeo import gdal
from common.minio_ops import connect_minio
from common.save_raster_artifact import save_raster_artifact
from common.convert_to_cog import tiff_to_cogtiff      

def _download_tifs_unique(
    client,
    bucket: str,
    prefix: str,
    workdir: str
) -> List[str]:
    tif_exts = (".tif", ".tiff")
    keys = sorted(
        obj.object_name
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True)
        if obj.object_name.lower().endswith(tif_exts)
    )

    if len(keys) < 2:
        raise RuntimeError(
            f"[ERROR] Found only {len(keys)} raster(s) at '{prefix}'. Need ≥ 2 to merge."
        )

    name_count: Dict[str, int] = {}
    local_paths: List[str] = []

    for key in keys:
        base = os.path.basename(key)           
        stem, ext = os.path.splitext(base)       

        n = name_count.get(stem, 0)
        name_count[stem] = n + 1
        if n > 0:
            base = f"{stem}_{n}{ext}"            

        dst_path = os.path.join(workdir, base)
        client.fget_object(bucket, key, dst_path)
        local_paths.append(dst_path)

    return local_paths

def merge_rasters(
    config_path: str,
    client_id: str,
    prefix: str,
    store_artifact: str = "minio",
    file_path: str | None = None,
) -> str:
    """
    Merge every raster under <client_id>/<prefix>/ recursively into a single COG.

    Parameters
    ----------
    config_path   : str (Reactflow will ignore this parameter)
    client_id     : str (Reactflow will translate it as input)
    prefix        : str (Reactflow will take it from the previous step)
    store_artifact: str (Reactflow will ignore this parameter)
    file_path     : str (Reactflow will ignore this parameter)
    """
    client = connect_minio(config_path, client_id)

    with tempfile.TemporaryDirectory() as tmp:
        local_tifs = _download_tifs_unique(
            client, client_id, prefix.rstrip("/") + "/", tmp
        )

        vrt_path = os.path.join(tmp, "merged.vrt")
        gdal.BuildVRT(vrt_path, local_tifs)

        cog_local = os.path.join(tmp, "merged_cog.tif")
        tiff_to_cogtiff(vrt_path, cog_local)

        with io.StringIO() as _buf, redirect_stdout(_buf):
            save_raster_artifact(
                config=config_path,
                client_id=client_id,
                local_path=cog_local,
                file_path=file_path,
                store_artifact=store_artifact,
            )
            print(f"{file_path}")
        return file_path if store_artifact.lower() == "minio" else cog_local
