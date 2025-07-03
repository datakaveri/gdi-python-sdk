import geopandas as gpd
import fiona
import os
from common.minio_ops import connect_minio, stream_to_minio
import io

fiona.supported_drivers['KML'] = 'rw'


def convert_format(config : str, client_id : str, store_artifact : str, input_vector: str, file_path: str ) -> None:
   
    # Edge cases
   if os.path.splitext(input_vector)[1] == os.path.splitext(file_path)[1]:
        print("The original and new filenames have the same extension. No conversion needed.")
        return
   if not isinstance(input_vector, str) or not isinstance(file_path, str):
        raise TypeError("Both input_vector and file_path must be strings.")
   
   if not input_vector or not file_path:
        raise ValueError("Both input_vector and file_path must be provided and cannot be empty.")

   if not os.path.exists(input_vector):
        raise FileNotFoundError(f"The original file '{input_vector}' does not exist.")
   
   if os.path.splitext(input_vector)[1] not in ['.geojson', '.shp', '.gpkg', '.kml']:
        raise ValueError("Unsupported original file format. Supported formats are: .geojson, .shp, .gpkg, .kml")





   
   if store_artifact == "minio" :
        client = connect_minio(config, client_id)

   # conversion logic 
   if file_path.endswith('.geojson'):
        if store_artifact == "minio":
            with client.get_object(client_id, input_vector) as response:
                gdf = gpd.read_file(io.BytesIO(response.read()))
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            if gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs("EPSG:4326")

            gdf.to_file(file_path, driver='GeoJSON')
            stream_to_minio(client, client_id, file_path, file_path)
            os.remove(file_path)
        if store_artifact == "local":
           # Read the GeoDataFrame from the input vector file
            gdf = gpd.read_file(input_vector)
            gdf.to_file(file_path, driver='GeoJSON')

   if file_path.endswith('.shp'):
        if store_artifact == "minio":
            with client.get_object(client_id, input_vector) as response:
                gdf = gpd.read_file(io.BytesIO(response.read()))
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            if gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs("EPSG:4326")

            gdf.to_file(file_path)
            stream_to_minio(client, client_id, file_path, file_path)
            os.remove(file_path)
        if store_artifact == "local":
           # Read the GeoDataFrame from the input vector file
            gdf = gpd.read_file(input_vector)
            gdf.to_file(file_path)
    
   if file_path.endswith('.gpkg'):
        if store_artifact == "minio":
            with client.get_object(client_id, input_vector) as response:
                gdf = gpd.read_file(io.BytesIO(response.read()))
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            if gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs("EPSG:4326")

            gdf.to_file(file_path, driver='GPKG')
            stream_to_minio(client, client_id, file_path, file_path)
            os.remove(file_path)
        if store_artifact == "local":
           # Read the GeoDataFrame from the input vector file
            gdf = gpd.read_file(input_vector)
            gdf.to_file(file_path, driver='GPKG')
    
   if file_path.endswith('.kml'):
        if store_artifact == "minio":
            with client.get_object(client_id, input_vector) as response:
                gdf = gpd.read_file(io.BytesIO(response.read()))
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            if gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs("EPSG:4326")

            gdf.to_file(file_path, driver='KML')
            stream_to_minio(client, client_id, file_path, file_path)
            os.remove(file_path)
        if store_artifact == "local":
           # Read the GeoDataFrame from the input vector file
            gdf = gpd.read_file(input_vector)
            gdf.to_file(file_path, driver='KML')