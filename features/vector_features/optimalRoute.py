import os
import io
import uuid
import geopandas as gpd
import networkx as nx
from shapely.geometry import Point, LineString
from scipy.spatial import KDTree
from networkx.algorithms.approximation import traveling_salesman_problem
# from tqdm import tqdm

from common.minio_ops import connect_minio
from common.save_feature_artifact import save_feature


def compute_optimal_route(
    config: str,
    client_id: str,
    artifact_url: str,          # MinIO object name of the pickled road network
    points_file: str,           # Local file path to user's point data
    store_artifact: str,  # Whether to upload the output to MinIO
    file_path: str = None       # Base name for your route .pkl in MinIO
) -> dict:
    """
    Function to compute the optimal route through a road network for a set of input points.In editor it will be renamed as create-optimal-route.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    client_id : str (Reactflow will translate it as input)
    artifact_url : str (Reactflow will take it from the previous step)
    points_file : str (Reactflow will translate it as input)
    store_artifact : str (Reactflow will ignore this parameter)
    file_path : str (Reactflow will ignore this parameter, This parameter will be optoinal)
    """

    # 1. Connect to MinIO
    minio_client = connect_minio(config, client_id)
    # print("[INFO] Connected to MinIO successfully.")

    # 2. Download the road network (pickled GeoDataFrame) from MinIO
    import io
    try:
        with minio_client.get_object(bucket_name=client_id, object_name=artifact_url) as response:
            road_gdf = gpd.read_file(io.BytesIO(response.read()))
        # print(f"[INFO] Road network artifact '{artifact_url}' loaded from MinIO.")
    except Exception as e:
        raise RuntimeError(f"[ERROR] Unable to download/load road artifact from MinIO: {e}")

    # Explode multi-line geometries and remove empties
    road_gdf = road_gdf.explode(ignore_index=True)
    road_gdf = road_gdf[road_gdf.geometry.notna()]
    if road_gdf.empty:
        raise ValueError("[ERROR] Road GeoDataFrame is empty after filtering/explode.")

    # If no CRS on road_gdf, assume EPSG:4326
    if road_gdf.crs is None:
        # print("[WARN] Road network has no CRS; assuming EPSG:4326.")
        road_gdf.set_crs("EPSG:4326", inplace=True)

    # 3. Build bounding box & construct a NetworkX graph
    minx, miny, maxx, maxy = road_gdf.total_bounds
    # print(f"[INFO] Road bounding box: ({minx}, {miny}, {maxx}, {maxy})")

    G = nx.Graph()
    for _, row in road_gdf.iterrows():
        geom = row.geometry
        if geom.geom_type == "LineString":
            coords = list(geom.coords)
            for i in range(len(coords) - 1):
                node1, node2 = coords[i], coords[i + 1]
                dist = Point(node1).distance(Point(node2))
                G.add_edge(node1, node2, weight=dist)
    # print(f"[INFO] Created graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")

    # 4. Read local points file
    points_gdf = gpd.read_file(points_file)
    if points_gdf.empty:
        raise ValueError(f"[ERROR] No valid points found in '{points_file}'.")

    # If no CRS on points, assume EPSG:7755
    if points_gdf.crs is None:
        # print("[INFO] Points file has no CRS; assigning EPSG:7755 as default.")
        points_gdf.set_crs("EPSG:7755", inplace=True)

    # Reproject points if needed
    if road_gdf.crs != points_gdf.crs:
        # print("[WARN] CRS mismatch: reprojecting points to match road network CRS.")
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
    for idx, row in points_gdf.iterrows():
        pt = row.geometry
        x, y = pt.x, pt.y

        # Check bounding
        if not (minx <= x <= maxx and miny <= y <= maxy):
            raise ValueError(f"[ERROR] Point ({x},{y}) is outside the road bounding box.")

        _, nearest_idx = kd_tree.query([x, y])
        snapped_node = tuple(road_nodes_arr[nearest_idx])
        snapped_nodes.append(snapped_node)
        original_points.append(pt)

    # print(f"[INFO] Snapped {len(snapped_nodes)} input points to the road network.")

    # 6. Compute paths using A* instead of Dijkstra
    node_count = len(snapped_nodes)
    shortest_paths = {}
    
    # Define a heuristic function for A* using Euclidean distance
    heuristic = lambda u, v: Point(u).distance(Point(v))
    
    for i in range(node_count):
        for j in range(i + 1, node_count):
            src = snapped_nodes[i]
            dst = snapped_nodes[j]
            try:
                path = nx.astar_path(G, source=src, target=dst, heuristic=heuristic, weight="weight")
                distance = nx.astar_path_length(G, source=src, target=dst, heuristic=heuristic, weight="weight")
                shortest_paths[(i, j)] = {"path": path, "distance": distance}
            except nx.NetworkXNoPath:
                shortest_paths[(i, j)] = {"path": None, "distance": float("inf")}

    # Build TSP graph
    TSP_G = nx.Graph()
    for (i, j), info in shortest_paths.items():
        if info["path"] is not None:  # Only add edges if there's a valid path
            TSP_G.add_edge(i, j, weight=info["distance"])

    # Solve TSP
    tsp_order = traveling_salesman_problem(TSP_G, cycle=True)
    # print(f"[INFO] TSP visitation order (indices): {tsp_order}")

    # 7. Reconstruct final route
    tsp_full_path = []
    for i in range(len(tsp_order) - 1):
        start_idx = tsp_order[i]
        end_idx = tsp_order[i + 1]
        
        # Handle path retrieval based on the index order (maintain consistent key format)
        if start_idx < end_idx:
            pair_key = (start_idx, end_idx)
        else:
            pair_key = (end_idx, start_idx)
            
        if pair_key in shortest_paths and shortest_paths[pair_key]["path"] is not None:
            segment_path = shortest_paths[pair_key]["path"]
            # If we need to reverse the path
            if start_idx > end_idx:
                segment_path = segment_path[::-1]
            
            # Avoid duplicating nodes between segments
            if tsp_full_path and tsp_full_path[-1] == segment_path[0]:
                tsp_full_path.extend(segment_path[1:])
            else:
                tsp_full_path.extend(segment_path)

    if not tsp_full_path:
        raise ValueError("[ERROR] Could not construct a valid route through all points.")

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
    # 9. If store_artifact=local/minio, upload to MinIO or save locally as per user input
    # --------------------------------------------------------
    if store_artifact:
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=route_gdf, file_path=file_path, config_path=config)
        points_file_path = file_path.replace(".geojson", "_points.geojson")
        save_feature(client_id=client_id, store_artifact=store_artifact, gdf=points_ordered_gdf, file_path=points_file_path, config_path=config)

    else:
        pass
    #     print("Data not saved. Set store_artifact to minio/local to save the data to minio or locally.")
    #     print("Computed optimal route successfully")

    # print("[INFO] TSP route computation complete!")
    return 