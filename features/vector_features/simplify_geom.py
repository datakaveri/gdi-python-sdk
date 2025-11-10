import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely import simplify
from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature
import io


def simplify_geometry_DP(config: str, client_id: str, artifact_url: str, store_artifact: str, file_path: str = None, tolerance: float = 1.0, preserve_topology: bool = True) -> None:
    """
    Simplifies geometries (LineString or Polygon only) using using the Douglas-Peucker algorithm. Optionally saves the result to MinIO or locally.In editor it will be renamed as simplify-geometry.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter)
    tolerance: float (Reactflow will translate it as input, This parameter will be optional)
    preserve_topology: enum [True, False] (Reactflow will translate it as input, This parameter will be optional)
    """

    client = connect_minio(config, client_id)

    try:
        # --- Step 1: Read GeoDataFrame from MinIO ---
        with client.get_object(client_id, artifact_url) as response:
            gdf = gpd.read_file(io.BytesIO(response.read()))

        # --- Step 2: Ensure CRS and reproject to EPSG:7755 ---
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        gdf = gdf.to_crs(epsg=7755)

        # --- Step 3: Identify geometry type ---
        geometry_type = gdf.geometry.iloc[0].geom_type
        # print(f"Geometry type identified: {geometry_type}")

        # --- Step 4: Validation for geometry types ---
        if geometry_type == "Point":
            raise ValueError("Input geometry must be LineString or Polygon, not Point.")
        elif geometry_type in ["LineString", "MultiLineString"]:
            # For lines, only tolerance should be used — ignore preserve_topology
            # print("Simplifying line geometry (only 'tolerance' is used).")
            simplified_geoms = [simplify(geom, tolerance=tolerance) for geom in gdf.geometry]
        elif geometry_type in ["Polygon", "MultiPolygon"]:
            # For polygons, use both tolerance and preserve_topology
            # print(f"Simplifying polygon geometry (tolerance={tolerance}, preserve_topology={preserve_topology}).")
            simplified_geoms = [
                simplify(geom, tolerance=tolerance, preserve_topology=preserve_topology)
                for geom in gdf.geometry
            ]
        else:
            raise ValueError(f"Unsupported geometry type: {geometry_type}")

        # --- Step 5: Replace geometries ---
        gdf["geometry"] = simplified_geoms
        # print("Simplification completed successfully.")

        # --- Step 6: Save simplified GeoDataFrame ---
        if store_artifact:
            save_feature(
                client_id=client_id,
                store_artifact=store_artifact,
                gdf=gdf,
                file_path=file_path,
                config_path=config
            )
            # print("Simplified geometries saved successfully.")
        else:
            print("Data not saved. Set 'store_artifact' to 'minio' or 'local' to save results.")

    except Exception as e:
        raise RuntimeError(f"Error during geometry simplification: {e}")
