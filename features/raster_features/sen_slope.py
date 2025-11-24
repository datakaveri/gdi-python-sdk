import os
import io
import tempfile
import shutil
import pandas as pd
import numpy as np
import subprocess
from osgeo import gdal, osr
import sys
import time

from common.minio_ops import connect_minio
from common.convert_to_cog import tiff_to_cogtiff
from common.save_raster_artifact import save_raster_artifact


# -------------------------------
# Constants
# -------------------------------
TARGET_EPSG = 7755
NODATA = -9999.0


# -------------------------------
# Utility functions (corrected)
# -------------------------------

def _run_subprocess(cmd):
    """Run subprocess silently and raise a helpful error on failure."""
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Command failed: {' '.join(map(str, cmd))}\n"
            f"Return code: {e.returncode}\n"
        ) from e



def _fix_nodata_and_reproject(raster_path, tmp_dir, python_exec, scripts_dir, target_epsg=TARGET_EPSG, nodata=NODATA):
    """
    Ensure raster has the desired nodata, projection and filled gaps.
    Returns path to processed raster (filled).
    """
    ds = gdal.Open(raster_path)
    if ds is None:
        raise FileNotFoundError(f"Cannot open raster: {raster_path}")

    try:
        band = ds.GetRasterBand(1)
        curr_nodata = band.GetNoDataValue()
        proj = osr.SpatialReference(wkt=ds.GetProjection())
        epsg = proj.GetAttrValue("AUTHORITY", 1)
    finally:
        # close dataset to release lock
        ds = None

    base = os.path.basename(raster_path)
    work_file = raster_path

    # Step: Fix NoData if needed
    if curr_nodata is None or float(curr_nodata) != float(nodata):
        nd = os.path.join(tmp_dir, f"nodata_{base}")
        cmd = ["gdal_translate", "-a_nodata", str(nodata), work_file, nd]
        _run_subprocess(cmd)
        work_file = nd

    # Step: Reproject if needed
    if epsg is None or str(epsg) != str(target_epsg):
        rp = os.path.join(tmp_dir, f"reproj_{base}")
        cmd = [
            "gdalwarp", "-t_srs", f"EPSG:{target_epsg}",
            "-r", "near",
            "-dstnodata", str(nodata),
            work_file, rp
        ]
        _run_subprocess(cmd)
        work_file = rp

    # Step: Fill NoData using gdal_fillnodata.py
    # scripts_dir should point to the directory containing gdal_fillnodata.py (usually the venv Scripts folder)
    fill_script = os.path.join(scripts_dir, "gdal_fillnodata.py")
    if not os.path.exists(fill_script):
        # Try alternative common locations (safe fallback)
        possible = [
            os.path.join(os.path.dirname(sys.executable), "gdal_fillnodata.py"),
            os.path.join(os.path.dirname(sys.executable), "..", "Scripts", "gdal_fillnodata.py"),
        ]
        found = None
        for p in possible:
            p = os.path.normpath(p)
            if os.path.exists(p):
                found = p
                break
        if found:
            fill_script = found
        else:
            raise FileNotFoundError(f"gdal_fillnodata.py not found in {scripts_dir} or common locations. "
                                    f"Checked: {fill_script} and {possible}")

    filled = os.path.join(tmp_dir, f"filled_{base}")
    cmd = [python_exec, fill_script, "-md", "5", work_file, filled]
    _run_subprocess(cmd)

    if not os.path.exists(filled):
        raise FileNotFoundError(f"Filled raster not created: {filled}")

    return filled


def _compute_pairwise_slope(r1, r2, out_file, delta_t_days, python_exec, scripts_dir, nodata=NODATA):
    """Compute slope = (r2 - r1) / Δt using gdal_calc.py"""
    calc_script = os.path.join(scripts_dir, "gdal_calc.py")
    if not os.path.exists(calc_script):
        # try alternative location
        alt = os.path.join(os.path.dirname(sys.executable), "gdal_calc.py")
        if os.path.exists(alt):
            calc_script = alt
        else:
            raise FileNotFoundError(f"gdal_calc.py not found at {calc_script} or {alt}")

    calc_expr = f"where((A!={int(nodata)}) & (B!={int(nodata)}), (A-B)/{delta_t_days}, {int(nodata)})"
    cmd = [
        python_exec, calc_script,
        "-A", r2, "-B", r1,
        "--outfile=" + out_file,
        f"--calc={calc_expr}",
        "--type=Float32",
        f"--NoDataValue={int(nodata)}"
    ]
    _run_subprocess(cmd)

    if not os.path.exists(out_file):
        raise RuntimeError("Slope output not created: " + out_file)


def _median_stack(rasters, output, nodata=NODATA):
    """Compute median of slope stack (Sen's slope). Close datasets after use to avoid file locks."""
    if not rasters:
        raise ValueError("No rasters provided to median stack")

    ref = gdal.Open(rasters[0])
    if ref is None:
        raise FileNotFoundError(f"Cannot open reference raster: {rasters[0]}")

    geotrans = ref.GetGeoTransform()
    proj = ref.GetProjection()
    ref_x = ref.RasterXSize
    ref_y = ref.RasterYSize
    # close ref now to avoid lock while writing output
    ref = None

    arrs = []
    for r in rasters:
        ds = gdal.Open(r)
        if ds is None:
            raise FileNotFoundError(f"Cannot open slope raster: {r}")
        try:
            band = ds.GetRasterBand(1)
            arr = band.ReadAsArray().astype(np.float32)
            ndv = band.GetNoDataValue()
            if ndv is None:
                ndv = nodata
            arr[arr == ndv] = np.nan
            arrs.append(arr)
        finally:
            # ensure dataset is closed to release file handle
            ds = None

    arr_stack = np.stack(arrs, axis=0)
    median_arr = np.nanmedian(arr_stack, axis=0)

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(output, ref_x, ref_y, 1, gdal.GDT_Float32)
    if out_ds is None:
        raise RuntimeError(f"Unable to create output raster: {output}")

    out_ds.SetGeoTransform(geotrans)
    out_ds.SetProjection(proj)
    band_out = out_ds.GetRasterBand(1)
    write_arr = np.where(np.isnan(median_arr), nodata, median_arr).astype(np.float32)
    band_out.WriteArray(write_arr)
    band_out.SetNoDataValue(nodata)
    band_out.FlushCache()
    # close output
    out_ds = None


# -------------------------------------------------
# Main Processing Function (corrected)
# -------------------------------------------------

def compute_sen_slope(
    config: str,
    client_id: str,
    artifact_url: str,
    store_artifact: str,
    file_path: str = None
):
    """
    Function to slope sensitivity for timeseries data and the stac_datetime utility is must before this to get datetime stamp of all rasters. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as senslope.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config, client_id)

    # -----------------------------
    # Step 1: Read CSV from MinIO
    # -----------------------------
    with client.get_object(client_id, artifact_url) as response:
        csv_bytes = response.read()

    df = pd.read_csv(io.BytesIO(csv_bytes))
    if "filepath" not in df.columns or "datetime" not in df.columns:
        raise ValueError("CSV must have 'filepath' and 'datetime' columns")

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)

    if len(df) < 2:
        raise RuntimeError("Need at least 2 rasters to compute Sen slope")

    # -----------------------------
    # Prepare working dir and helpers
    # -----------------------------
    tmp_dir = tempfile.mkdtemp(prefix="senslope_")
    python_exec = sys.executable
    # scripts_dir should be the folder containing gdal_calc.py and gdal_fillnodata.py (usually venv Scripts)
    scripts_dir = os.path.dirname(python_exec)

    local_rasters = []
    cleaned = []
    slope_files = []

    try:
        # -----------------------------
        # Step 2: Download rasters
        # -----------------------------
        for fp in df["filepath"]:
            # ensure nested dirs exist locally
            local_basename = os.path.basename(fp)
            local_fp = os.path.join(tmp_dir, local_basename)

            # client.get_object returns a file-like object; use copyfileobj for streaming
            obj = client.get_object(client_id, fp)
            with open(local_fp, "wb") as out_f:
                shutil.copyfileobj(obj, out_f)
            # close obj if it exposes close
            try:
                obj.close()
            except Exception:
                pass

            local_rasters.append(local_fp)

        # -----------------------------
        # Step 3: Fix NoData + Reproject + Fill
        # -----------------------------
        for r in local_rasters:
            proc = _fix_nodata_and_reproject(r, tmp_dir, python_exec, scripts_dir, target_epsg=TARGET_EPSG, nodata=NODATA)
            cleaned.append(proc)

        # -----------------------------
        # Step 4: Compute slopes between consecutive rasters
        # Use actual delta days between datetimes
        # -----------------------------
        for i in range(len(cleaned) - 1):
            t1 = df.loc[i, "datetime"]
            t2 = df.loc[i + 1, "datetime"]
            # compute exact delta in days as float; avoid zero by fallback to 1
            delta_seconds = (t2 - t1).total_seconds()
            delta_days = delta_seconds / 86400.0 if delta_seconds != 0 else 1.0

            out_slope = os.path.join(tmp_dir, f"slope_{i}.tif")
            _compute_pairwise_slope(cleaned[i], cleaned[i + 1], out_slope, delta_days, python_exec, scripts_dir, nodata=NODATA)
            slope_files.append(out_slope)

        # -----------------------------
        # Step 5: Median stack (Sen's slope result)
        # -----------------------------
        final_raw = os.path.join(tmp_dir, "sens_slope_raw.tif")
        _median_stack(slope_files, final_raw, nodata=NODATA)

        # -----------------------------
        # Step 6: Convert to COG
        # -----------------------------
        final_cog = os.path.join(tmp_dir, "sens_slope_cog.tif")
        tiff_to_cogtiff(final_raw, final_cog)

        # -----------------------------
        # Step 7: Save (local or MinIO)
        # -----------------------------
        if store_artifact and store_artifact.lower() in ("minio", "local"):
            save_raster_artifact(
                config=config,
                client_id=client_id,
                local_path=final_cog,
                file_path=file_path,
                store_artifact=store_artifact
            )
            print(f"{file_path}")
        else:
            print("Data not saved. Set store_artifact to minio/local to save the data.")
            print("Sen's slope computed successfully and available in temporary folder.")

    except Exception as e:
        # print a helpful message and re-raise
        raise RuntimeError(f"Error computing Sen's slope: {e}") from e

    finally:
        # -----------------------------
        # Step 8: Cleanup - ensure files closed before deletion
        # -----------------------------
        # small wait to allow OS to release handles on Windows
        time.sleep(0.1)
        try:
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
        except Exception as cleanup_err:
            # attempt to individually remove files if rmtree fails (common on Windows due to locks)
            try:
                for root, dirs, files in os.walk(tmp_dir, topdown=False):
                    for name in files:
                        p = os.path.join(root, name)
                        try:
                            os.remove(p)
                        except Exception:
                            pass
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except Exception:
                            pass
                if os.path.isdir(tmp_dir):
                    os.rmdir(tmp_dir)
            except Exception:
                # last resort: ignore cleanup error but warn
                print(f"[WARN] Failed to fully remove temp dir {tmp_dir}: {cleanup_err}")
