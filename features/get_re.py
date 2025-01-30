import requests
from auth.token_gen import TokenGenerator
from common.minio_ops import connect_store_minio
import geopandas as gpd

class ResourceFetcher:
    def __init__(self, client_id: str, client_secret: str, role: str):
        """
        Initialize the ResourceFetcher with authentication details.

        :param client_id: The client ID for authentication.
        :param client_secret: The client secret for authentication.
        :param token_url: The URL to fetch the token from.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.role = role
        

    def fetch_resource_data(self, resource_id:str ,save_object : bool = False,  config_path:str = None ,file_path : str = None ) -> dict:
        """
        Fetch data for a specified resource using the generated token.
        resource_id : str : The ID of the resource to fetch data from.
        config_path : str : The path to the minio configuration file.
        file_path : str : The path to save the fetched data.
        offset : int : The offset to fetch the data from.
        save_object : bool : Whether to save the fetched data to minio or not.

        """
        try:
            # Generate the token
            token_generator = TokenGenerator(self.client_id, self.client_secret,self.role)
            auth_token = token_generator.generate_token()

            # Fetch resource data
            resource_url = f"https://geoserver.dx.gsx.org.in/collections/{resource_id}/items?offset=1"
            headers = {"Authorization": f"Bearer {auth_token}"}

            response = requests.get(resource_url, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            
            gdf = gpd.GeoDataFrame(response.json()['features'])

            if save_object:
                try:
                    connect_store_minio(config_path, self.client_id, response.json(), file_path)
                except Exception as e:
                    raise Exception(f"Error while saving file: {e}")
            else:
                print("Data not saved. Set save_object to True , provide the minio config path and file_path to save the data to minio.")

            return gdf  # Return the fetched data as a geopandas dataframe
        except requests.RequestException as e:
            raise Exception(f"Error fetching resource data: {e}")

   


