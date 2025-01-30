import requests
from auth.token_gen import TokenGenerator
from minio_con import connect_store_minio
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
   


# if __name__ == "__main__":
#     client_id = "7dcf1193-4237-48a7-a5f2-4b530b69b1cb"
#     client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
#     token_url = "https://dx.gsx.org.in/auth/v1/token"
#     item_id = "geoserver.dx.ugix.org.in"
#     item_type = "resource_server"
#     role = "consumer"
#     resource_id = "95886a22-704d-4922-815c-39af80acd520"
#     offset = 1

#     fetcher = ResourceFetcher(client_id, client_secret, role=role)
#     try:
#         resource_data = fetcher.fetch_resource_data(resource_id=resource_id, save_object=True, config_path='config.json', file_path='data_3.gpkg')
        
#     except Exception as e:
#         print(e)
