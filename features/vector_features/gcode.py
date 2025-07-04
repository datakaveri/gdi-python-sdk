from geopy.geocoders import Nominatim
import requests

# Get the bounding box of a location
def get_bounding_box(location: str):
    try:
        geolocator = Nominatim(user_agent="gdi_app")
        location = geolocator.geocode(location)
        bb = location.raw['boundingbox']
        order = [2, 1, 3, 0]
        bb = [round(float(bb[i]), 6) for i in order]
        return bb
    except Exception as e:
        raise e

# Query the GeoServer API for data
def list_available_features(name: str) -> dict:
    """
    Function to query the GeoServer API for data within a bounding box.
    Parameters:
    ----------
    name : str
        Location name to fetch data for.
    """
    bbox = get_bounding_box(name)

    base_url = "https://dx.geospatial.org.in/dx/cat/v1/search"
    params = {
        "geoproperty": "location",
        "georel": "intersects",
        "geometry": "bbox",
        "coordinates": f"[[{bbox[0]},{bbox[1]}],[{bbox[2]},{bbox[3]}]]",
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        return response.json()['results']
    else:
        return {"error": f"Failed to fetch data, status code: {response.status_code}, response: {response.text}"}

# Function to print features with serial number, label, and description
def list_features(location_name: str):
    data = list_available_features(location_name)

    if isinstance(data, list) and data:
        for i, item in enumerate(data, start=1):
            label = item.get("label", "N/A")
            description = item.get("description", "N/A")
            print(f"{i}. Label: {label}")
            print(f"   Description: {description}\n")
    else:
        print("No data found or an error occurred:")
        print(data)

# if __name__ == "__main__":
#     list_features("Varanasi")
