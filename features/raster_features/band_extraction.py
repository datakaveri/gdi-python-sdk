import re

def band_extraction(
    asset_list: str,
    item_key: str,
    asset_key: str,
) -> str:
    """
    From the string of filepaths, extract path to particular band of interest.In editor it will be renamed as extract-band-path.

    Parameters
    ----------
    asset_list  : str (Reactflow will take it from the previous step)
    item_key : str (Reactflow will translate it as input)
    asset_key : str (Reactflow will translate it as input)
    """
    # Parse paths
    file_paths = [fp.strip() for fp in asset_list.split("$") if fp.strip()]

    # Pattern to match asset key (ignoring case and whitespace/underscore)
    asset_pattern = re.compile(re.sub(r"\s+", "", asset_key), re.IGNORECASE)

    # Find matching path
    final_path = None
    for path in file_paths:
        if item_key.lower() in path.lower():
            filename = path.split("/")[-1]
            normalized_filename = re.sub(r"[ _]", "", filename)
            if asset_pattern.search(normalized_filename):
                final_path = path
                break

    # Print result
    if final_path:
        print(final_path)
    else:
        print("No matching path found.")


# asset_list = 'downloaded_from_stac/C3_MX_20240421_2484919101/C3_MX_20240421_2484919101_BAND3_cog.tif$downloaded_from_stac/C3_MX_20240421_2484919101/C3_MX_20240421_2484919101_BAND1_cog.tif$downloaded_from_stac/C3_MX_20240421_2484919101/C3_MX_20240421_2484919101_BAND4_cog.tif$downloaded_from_stac/C3_MX_20240421_2484919101/C3_MX_20240421_2484919101_BAND2_cog.tif'
# item_key = "C3_MX_20240421_2484919101"
# asset_key = "Band 1"
# band_extraction(asset_list, item_key, asset_key)
