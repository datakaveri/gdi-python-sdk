from geopy.geocoders import Nominatim
import requests

#get the bounding box of a location
def get_bounding_box(location:str):
    try:
        geolocator = Nominatim(user_agent="gdi_app")
        location = geolocator.geocode(location)
        bb = location.raw['boundingbox']
        order = [2,1,3,0]
        bb = [round(float(bb[i]),6) for i in order]
        return bb
    except Exception as e:
        raise e
    

# query the geoserver api for data
def list_features(name: str) -> dict:

    """
     Function to query the geoserver api for data within a bounding box.In editor it will be renamed as list-vector-data.

     Parameters:
     ----------
     name : str (Reactflow will translate it as input)
    """


    bbox = get_bounding_box(name)
    
    base_url = "https://dx.gsx.org.in/ugix/cat/v1/search"
    params = {
        "geoproperty": "location",
        "georel": "within",
        "geometry": "bbox",
        "coordinates": f"[[{bbox[0]},{bbox[1]}],[{bbox[2]},{bbox[3]}]]",
    }
    
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        return response.json()['results']
    else:
        return {"error": f"Failed to fetch data, status code: {response.status_code}, response: {response.text}"}

# if __name__ == "__main__":
#     # Example bounding box: [min_lon, min_lat, max_lon, max_lat]
#     name = "telangana"
#     data = query_api(name)
#     print(data['results'])
