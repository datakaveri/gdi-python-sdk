import geopandas as gpd
from common.minio_ops import connect_minio, get_bucket_name
import warnings

warnings.filterwarnings("ignore")
import os
import io
from datetime import timedelta
import uuid


def download_features(config: str, artifact_url: str, save_as: str) -> str:
    """
    Download features from the minio bucket and save it as a geopackage file.In editor it will be renamed as download-vector-features.
    Parameters
    ----------
    config : str (Reactflow will ignore this parameter)
    artifact_url : str (Reactflow will take it from the previous step)
    save_as : str (Reactflow will ignore this parameter)
    """

    client = connect_minio(config)
    bucket_name = get_bucket_name(config)

    try:
        with client.get_object(bucket_name, artifact_url) as response:
            data = gpd.read_file(io.BytesIO(response.read()))
            data.to_file("temp.geojson", driver="GeoJSON")
    except Exception as e:
        print(e)

    try:
        if save_as is None:
            save_as = f"downloadable/{str(uuid.uuid4())}.geojson"

        client.fput_object(bucket_name, save_as, "temp.geojson")
        pre_signed_url = client.get_presigned_url(
            "GET", bucket_name, save_as, expires=timedelta(days=7)
        )
        os.remove("temp.geojson")
        print(pre_signed_url)
    except Exception as e:
        raise e


# download_features(config = '../../config.json', client_id = '7dcf1193-4237-48a7-a5f2-4b530b69b1cb', artifact_url = 'intermediate/paginated_data.pkl', save_as = 'intermediate/features_1_temp.geojson')
