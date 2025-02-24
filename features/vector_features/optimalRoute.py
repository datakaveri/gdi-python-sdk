# features/vector_features/optimalRoute.py

import os
import uuid
import pickle
import geopandas as gpd
import networkx as nx
from shapely.geometry import Point, LineString
from scipy.spatial import KDTree
from networkx.algorithms.approximation import traveling_salesman_problem
from tqdm import tqdm

from common.minio_ops import connect_minio


def compute_optimal_route(
    config: str,
    client_id: str,
    artifact_url: str,          # MinIO object name of the pickled road network
    points_file: str,           # Local file path to user's point data
    store_artifact: bool = False,  # Whether to upload the output to MinIO
    file_path: str = None       # Base name for your route .pkl in MinIO
) -> dict:
    """
    Reads:
      1) A road network artifact from MinIO (pickled GeoDataFrame).
      2) A local file with point features (GeoJSON, Shapefile, GPKG, etc.).
    Steps:
      - Loads the road network, explodes multiline geometries, checks CRS.
      - Builds a graph.
      - Reads user points, reprojects to match the road network if necessary.
      - Checks each point is within the road bounding box.
      - Snaps points to the nearest node in the road network.
      - Computes a TSP route for these points.
      - Writes the final route and an ordered points layer to local ".pkl" files:
          "temp_route.pkl" and "temp_points.pkl"
      - If store_artifact=True:
          * If no file_path was given, generate one as "optimal_route_<uuid>.pkl"
          * Upload "temp_route.pkl" to MinIO as file_path
          * Upload "temp_points.pkl" to MinIO as file_path.replace(".pkl", "_points.pkl")
          * Remove local .pkl files
      - If store_artifact=False:
          * Leaves the .pkl files on disk.
          * Prints a message telling the user the files are not uploaded.

    Returns
    -------
    dict
        {
          "route": <either MinIO key or local path>,
          "points": <either MinIO key or local path>,
        }
    """

    # 1. Connect to MinIO
    minio_client = connect_minio(config, client_id)
    print("[INFO] Connected to MinIO successfully.")

    # 2. Download the road network (pickled GeoDataFrame) from MinIO
    import io
    try:
        with minio_client.get_object(bucket_name=client_id, object_name=artifact_url) as response:
            road_gdf = pickle.loads(response.read())
        print(f"[INFO] Road network artifact '{artifact_url}' loaded from MinIO.")
    except Exception as e:
        raise RuntimeError(f"[ERROR] Unable to download/load road artifact from MinIO: {e}")

    # Explode multi-line geometries and remove empties
    road_gdf = road_gdf.explode(ignore_index=True)
    road_gdf = road_gdf[road_gdf.geometry.notna()]
    if road_gdf.empty:
        raise ValueError("[ERROR] Road GeoDataFrame is empty after filtering/explode.")

    # If no CRS on road_gdf, assume EPSG:4326
    if road_gdf.crs is None:
        print("[WARN] Road network has no CRS; assuming EPSG:4326.")
        road_gdf.set_crs("EPSG:4326", inplace=True)

    # 3. Build bounding box & construct a NetworkX graph
    minx, miny, maxx, maxy = road_gdf.total_bounds
    print(f"[INFO] Road bounding box: ({minx}, {miny}, {maxx}, {maxy})")

    G = nx.Graph()
    for _, row in tqdm(road_gdf.iterrows(), total=len(road_gdf), desc="Building graph"):
        geom = row.geometry
        if geom.geom_type == "LineString":
            coords = list(geom.coords)
            for i in range(len(coords) - 1):
                node1, node2 = coords[i], coords[i + 1]
                dist = Point(node1).distance(Point(node2))
                G.add_edge(node1, node2, weight=dist)
    print(f"[INFO] Created graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

    # 4. Read local points file
    points_gdf = gpd.read_file(points_file)
    if points_gdf.empty:
        raise ValueError(f"[ERROR] No valid points found in '{points_file}'.")

    # If no CRS on points, assume EPSG:7755
    if points_gdf.crs is None:
        print("[INFO] Points file has no CRS; assigning EPSG:7755 as default.")
        points_gdf.set_crs("EPSG:7755", inplace=True)

    # Reproject points if needed
    if road_gdf.crs != points_gdf.crs:
        print("[WARN] CRS mismatch: reprojecting points to match road network CRS.")
        try:
            points_gdf = points_gdf.to_crs(road_gdf.crs)
        except Exception as reproj_err:
            raise ValueError(f"[ERROR] Could not reproject points: {reproj_err}")

    # Filter valid Points
    points_gdf = points_gdf[points_gdf.geometry.notna() & (points_gdf.geom_type == "Point")]
    if points_gdf.empty:
        raise ValueError("[ERROR] After filtering, no valid Point geometry remains in the points file.")

    # 5. Snap each point to the nearest node in the graph
    import numpy as np
    road_nodes_arr = list(G.nodes())
    if not road_nodes_arr:
        raise ValueError("[ERROR] The road graph has no nodes. Cannot proceed.")

    kd_tree = KDTree(np.array(road_nodes_arr))

    snapped_nodes = []
    original_points = []
    for idx, row in tqdm(points_gdf.iterrows(), total=len(points_gdf), desc="Snapping points"):
        pt = row.geometry
        x, y = pt.x, pt.y

        # Check bounding
        if not (minx <= x <= maxx and miny <= y <= maxy):
            raise ValueError(f"[ERROR] Point ({x},{y}) is outside the road bounding box.")

        _, nearest_idx = kd_tree.query([x, y])
        snapped_node = tuple(road_nodes_arr[nearest_idx])
        snapped_nodes.append(snapped_node)
        original_points.append(pt)

    print(f"[INFO] Snapped {len(snapped_nodes)} input points to the road network.")

    # 6. Compute TSP route
    node_count = len(snapped_nodes)
    shortest_paths = {}
    for i in tqdm(range(node_count), desc="Row in matrix"):
        for j in range(i + 1, node_count):
            src = snapped_nodes[i]
            dst = snapped_nodes[j]
            path = nx.shortest_path(G, source=src, target=dst, weight="weight")
            distance = nx.shortest_path_length(G, source=src, target=dst, weight="weight")
            shortest_paths[(i, j)] = {"path": path, "distance": distance}

    # Build TSP graph
    TSP_G = nx.Graph()
    for (i, j), info in shortest_paths.items():
        TSP_G.add_edge(i, j, weight=info["distance"])

    # Solve TSP
    tsp_order = traveling_salesman_problem(TSP_G, cycle=True)
    print(f"[INFO] TSP visitation order (indices): {tsp_order}")

    # 7. Reconstruct final route
    tsp_full_path = []
    for i in tqdm(range(len(tsp_order) - 1), desc="Building final route"):
        start_node = snapped_nodes[tsp_order[i]]
        end_node   = snapped_nodes[tsp_order[i + 1]]
        segment_path = nx.shortest_path(G, source=start_node, target=end_node, weight="weight")
        tsp_full_path.extend(segment_path)

    route_line = LineString(tsp_full_path)
    route_gdf = gpd.GeoDataFrame(geometry=[route_line], crs=road_gdf.crs)

    # 8. Create ordered points (skip last duplicate)
    visited_points = []
    for rank, idx_val in enumerate(tsp_order[:-1], start=1):
        visited_points.append({
            "geometry": original_points[idx_val],
            "order": rank
        })
    points_ordered_gdf = gpd.GeoDataFrame(visited_points, crs=road_gdf.crs)

    # --------------------------------------------------------
    # 9. Save route & points to temporary local .pkl files
    # --------------------------------------------------------
    route_tempfile = "temp_route.pkl"
    points_tempfile = "temp_points.pkl"

    # Write to local .pkl
    route_gdf.to_pickle(route_tempfile)
    points_ordered_gdf.to_pickle(points_tempfile)

    # Prepare final result dict (we'll fill in once we decide local or MinIO)
    result = {
        "route": route_tempfile,
        "points": points_tempfile
    }

    # --------------------------------------------------------
    # 10. If store_artifact=True, upload to MinIO & remove locals
    # --------------------------------------------------------
    if store_artifact:
        if not file_path:
            # If user didn't specify a file_path, let's generate one
            file_path = f"optimal_route_{uuid.uuid4().hex}.pkl"

        points_file_path = file_path.replace(".pkl", "_points.pkl")

        try:
            # Upload route
            minio_client.fput_object(client_id, file_path, route_tempfile)
            print(f"[INFO] Uploaded route to MinIO as '{file_path}'")

            # Upload points
            minio_client.fput_object(client_id, points_file_path, points_tempfile)
            print(f"[INFO] Uploaded points to MinIO as '{points_file_path}'")

            # Remove local .pkl files
            os.remove(route_tempfile)
            os.remove(points_tempfile)

            # Update result to show MinIO object keys
            result["route"] = file_path
            result["points"] = points_file_path

        except Exception as e:
            raise Exception(f"[ERROR] Failed uploading files to MinIO: {e}")
    else:
        print("[INFO] Data not uploaded to MinIO. Set 'store_artifact' to True to save to MinIO.")
        print("[INFO] The .pkl files remain in your local directory.")

    print("[INFO] TSP route computation complete!")
    return result
