import requests
from auth.token_gen import TokenGenerator
from common.minio_ops import connect_store_minio
import geopandas as gpd
import uuid

class ResourceFetcher:
    def __init__(self, client_id: str, client_secret: str, role: str):
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.role = role
        

    def fetch_resource_data(self, resource_id:str ,save_object : bool = False,  config_path:str = None ,file_path : str = None ) -> dict:
       
        try:
            # Generate the token
            token_generator = TokenGenerator(self.client_id, self.client_secret,self.role)
            auth_token = token_generator.generate_token()

            # Fetch resource data
            resource_url = f"https://geoserver.dx.gsx.org.in/collections/{resource_id}/items?offset=1"
            headers = {"Authorization": f"Bearer {auth_token}"}

            response = requests.get(resource_url, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            
            gdf = gpd.GeoDataFrame.from_features(response.json()['features'])

            if save_object:
                if not file_path:
                    file_path = f"{uuid.uuid4()}.pkl"
                try:
                    connect_store_minio(config_path, self.client_id, gdf, file_path)
                except Exception as e:
                    raise Exception(f"Error while saving file: {e}")
            else:
                print("Data not saved. Set save_object to True , provide the minio config path and file_path to save the data to minio.")

            return gdf  # Return the fetched data as a geopandas dataframe
        except requests.RequestException as e:
            raise Exception(f"Error fetching resource data: {e}")



# client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --client-secret a863cafce5bd3d1bd302ab079242790d18cec974 --role consumer --resource-id 024b0c51-e44d-424c-926e-254b6c966978
# client_id =  '7dcf1193-4237-48a7-a5f2-4b530b69b1cb' 
# client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# role = "consumer"
# resource_id = "a4395596-14e6-4e17-83c4-989bc23fb3d2"
# save_object = True
# config_path = '../../config.json'
# file_path = 'intermediate/Road_Varanasi.pkl'
# fetcher = ResourceFetcher(client_id, client_secret, role)
# resource_data = fetcher.fetch_resource_data(resource_id, save_object, config_path, file_path)
   


