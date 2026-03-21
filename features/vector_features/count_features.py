import io
import geopandas as gpd
from common.minio_ops import connect_minio, get_bucket_name
import warnings

warnings.filterwarnings("ignore")


def count_features(config: str, artifact_url: str):
    """
    Function to count the number of features in a geodataframe.In editor it will be renamed as features-count.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    artifact_url : str (Reactflow will take it from the previous step)
    """

    client = connect_minio(config)
    bucket_name = get_bucket_name(config)

    try:
        with client.get_object(bucket_name, artifact_url) as response:
            data = gpd.read_file(io.BytesIO(response.read()))
            if data.empty:
                return "0"

            if "geometry" not in data.columns:
                raise ValueError("Invalid GeoDataFrame: no geometry column")


    except Exception as e:
        print(e)

    return str(len(data))
