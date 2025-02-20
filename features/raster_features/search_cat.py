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
    """Item level search for a given collection
    
    Parameters:
    -----------------
    collection_ids : list (nodered will read this as input)
    """

    client = Client.open("https://geoserver.dx.gsx.org.in/stac/")
    search = client.search(collections=collection_ids )

    for item in search.items_as_dicts():
        print(item['id'])
        print("Assets present in this item")
        for key in item['assets']:
            
            print(item['assets'][key]['href'])



def search_get_stac(collection_ids : list)->dict:
    """Item level search for a given collection
    
    Parameters:
    -----------------
    collection_ids : list (nodered will read this as input)
    """
    try:
        client = Client.open("https://geoserver.dx.gsx.org.in/stac/")
        search = client.search(collections=collection_ids )
    except Exception as e:
        raise e
    links = []
    asset = {}
    for item in search.items_as_dicts():
        if 'C3_MX' in item['id']:
           
            for a in item['assets']:
                if item['assets'][a]['type'] == 'image/tiff; application=geotiff':
                    links.append(item['assets'][a]['href'])
            asset.update({item['id']:links})
        links = [] 

    if search.items_as_dicts() is None:
        del links, search, client
        return None
    return asset

# collection_id = ['28e16f74-0ff8-4f11-a509-60fe078d8d47']
# # # search_stac(collection_id)

# links = search_get_stac(collection_id)
# pretty(links)