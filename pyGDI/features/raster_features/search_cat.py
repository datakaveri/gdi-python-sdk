from pystac_client import Client, ItemSearch
import warnings
warnings.filterwarnings("ignore")


def pretty(d, indent=0):
   for key, value in d.items():
      print('\t' * indent + str(key))
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print('\t' * (indent+1) + str(value))


def search_stac(collection_ids : list):
    """
    Item level search for a given collection
    
    Parameters
    -----------------
    collection_ids : list (nodered will read this as input)
    """

    client = Client.open("https://geoserver.dx.geospatial.org.in/stac/")
    search = client.search(collections=collection_ids )

    for item in search.items_as_dicts():
        print(item['id'])
        print("Assets present in this item")
        for key in item['assets']:
            
            print(item['assets'][key]['href'])



def search_get_stac(collection_ids: list) -> dict:
    
    try:
        client = Client.open("https://geoserver.dx.geospatial.org.in/stac/")
        search = client.search(collections=collection_ids)
    except Exception as e:
        raise e

    assets_dict = {}

    for item in search.items_as_dicts():
        if 'C3_MX' in item['id']:
            assets_info = {}

            for a in item['assets']:
                asset_data = item['assets'][a]
                if asset_data['type'] == 'image/tiff; application=geotiff':
                    assets_info[asset_data['title']] = asset_data['href']

            if assets_info:
                assets_dict[item['id']] = assets_info

    return assets_dict if assets_dict else None

# collection_id = ['28e16f74-0ff8-4f11-a509-60fe078d8d47']
# # # search_stac(collection_id)

# links = search_get_stac(collection_id)
# pretty(links)