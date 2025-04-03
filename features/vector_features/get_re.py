import requests
from auth.token_gen import TokenGenerator
from common.minio_ops import connect_store_minio
from common.paginator import fetch_paginated_data
import geopandas as gpd
import uuid
import io


class ResourceFetcher:
    def __init__(self, client_id: str, client_secret: str, role: str):
        
        self.client_id = client_id
        self.client_secret = client_secret
        self.role = role
        

    def fetch_resource_data(self, resource_id:str ,save_object : bool = False,  config_path:str = None ,file_path : str = None ) -> dict:
        """
        Function to fetch the resource data from the collections API using the resource_id.In editor it will be rename as fetch-resource.
        Parameters
        ----------
        client_id : str (Reactflow will translate it as input)
        client_secret : str (Reactflow will translate it as input)
        role : str (Reactflow will translate it as input)
        resource_id : str (Reactflow will translate it as input)
        save_object : enum [True, False] (Reactflow will translate it as input)
        config_path : str (Reactflow will translate it as input)
        file_path : str (Reactflow will translate it as input)
        """



       
        try:
            # Generate the token
            token_generator = TokenGenerator(self.client_id, self.client_secret,self.role)
            auth_token = token_generator.generate_token()

            # Request the collections API to get asset links 
            resource_url = f"https://geoserver.dx.geospatial.org.in/collections/{resource_id}"
            headers = {"Authorization": f"Bearer {auth_token}"}            
            response = requests.get(resource_url, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses
                  
            links = response.json().get("links")

            # getting the rel:enclosure link to get all the data in single shot            
            enclosure_link_arr = [link["href"] for link in links if link.get("rel") == "enclosure"]            
            resource_url = enclosure_link_arr[0] if enclosure_link_arr else None

            # iterative pagination incase if there is no rel:enclosure link
            if resource_url is None:
                for link in links:
                    if link.get("rel") == "items":                        
                        data = fetch_paginated_data(link["href"], headers)                     
            else:                
                response = requests.get(resource_url, headers=headers)
                response.raise_for_status()
                data = response.content
                
           
            
            gdf = gpd.read_file(io.BytesIO(data))# Read the fetched data as a geopandas dataframe

            if save_object:
                if not file_path:
                    file_path = f"{uuid.uuid4()}.pkl"
                try:
                    # gdf.to_file(file_path, driver='GeoJSON')
                    connect_store_minio(config_path, self.client_id, gdf, file_path)
                except Exception as e:
                    raise Exception(f"Error while saving file: {e}")
            else:
                print("Data not saved. Set save_object to True , provide the minio config path and file_path to save the data to minio.")
                print(gdf.info())

            return gdf  # Return the fetched data as a geopandas dataframe
        except requests.RequestException as e:
            raise Exception(f"Error fetching resource data: {e}")



# client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --client-secret a863cafce5bd3d1bd302ab079242790d18cec974 --role consumer --resource-id 024b0c51-e44d-424c-926e-254b6c966978
# client_id =  '7dcf1193-4237-48a7-a5f2-4b530b69b1cb' 
# client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# role = "consumer"
# resource_id = "024b0c51-e44d-424c-926e-254b6c966978"
# save_object = False
# config_path = "../config.json"
# file_path = "intermediate/data_new3.pkl"
# fetcher = ResourceFetcher(client_id, client_secret, role)
# resource_data = fetcher.fetch_resource_data(resource_id, save_object, config_path, file_path)
   


