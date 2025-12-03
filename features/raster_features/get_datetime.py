import os
import csv
import warnings
from pystac_client import Client
from auth.stac_token_gen import StacTokenGenerator
from common.minio_ops import connect_minio
from common.save_csv_artifact import save_csv_artifact


warnings.filterwarnings("ignore")


def get_datetime(
        client_id: str,
        client_secret: str,
        role: str,
        collection_id: str,
        folder_name: str,
        config: str,
        output_csv: str,
        store_artifact: str = "minio"
    ) -> None:
    """
    Function to fetch datetime for all raster assets inside a folder and write to CSV. Optionally upload the result back to MinIO or save locally.In editor it will be renamed as stac-datetime.
    Parameters
    ----------
    client_id : str (Reactflow will translate it as input)
    client_secret : str (Reactflow will translate it as input)
    role : enum [consumer, provider, admin] (Reactflow will translate it as input)
    collection_id : str (Reactflow will translate it as input)
    folder_name : str (Reactflow will take it from the previous step)
    config : str (Reactflow will ignore this parameter)
    store_artifact : str (Reactflow will ignore this parameter)
    output_csv : str (Reactflow will ignore this parameter)
    """

    # ------------------------------
    #  Generate STAC Token
    # ------------------------------
    token_gen = StacTokenGenerator(client_id, client_secret, role, collection_id)
    auth_token = token_gen.generate_token()
    headers = {"Authorization": f"Bearer {auth_token}"}

    # ------------------------------
    #  Query STAC for datetime values
    # ------------------------------
    client = Client.open("https://geoserver.dx.geospatial.org.in/stac/", headers=headers)
    search = client.search(collections=[collection_id])

    datetime_map = {}
    for item in search.items_as_dicts():
        datetime_map[item["id"]] = item["properties"].get("datetime")

    # ------------------------------
    #  Collect all file paths in the folder
    # ------------------------------
    file_list = []

    if store_artifact.lower() == "minio":
        try:
            minio_client = connect_minio(config, client_id)
            bucket_name = client_id  # GDI uses client_id as bucket
            objects = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)

            for obj in objects:
                if obj.object_name.endswith((".tif", ".tiff")):
                    file_list.append(obj.object_name)

        except Exception as e:
            raise Exception(f"Error listing objects from MinIO: {e}")

    elif store_artifact.lower() == "local":
        for root, _, files in os.walk(folder_name):
            for f in files:
                if f.endswith((".tif", ".tiff")):
                    file_list.append(os.path.join(root, f))

    else:
        raise ValueError("store_artifact must be either 'minio' or 'local'.")

    # ------------------------------
    #  Prepare CSV rows
    # ------------------------------
    csv_rows = [["filepath", "datetime"]]

    for fp in file_list:
        base = os.path.basename(fp)
        item_id = base.split("_")[0]     # infer item id
        dt = datetime_map.get(item_id, "NA")
        csv_rows.append([fp, dt])

    # ------------------------------
    #  Write CSV Locally First
    # ------------------------------
    temp_csv = "temp_datetime_list.csv"
    with open(temp_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(csv_rows)

    # ------------------------------
    #  Save CSV As Artifact
    # ------------------------------
    final_path = f"{output_csv}"

    if store_artifact.lower() == "minio":
        try:
            save_csv_artifact(
                config=config,
                client_id=client_id,
                local_path=temp_csv,
                file_path=final_path,
                store_artifact="minio"
            )
            # print(f"{final_path}")
        except Exception as e:
            raise Exception(f"Error saving CSV to MinIO: {e}")

    elif store_artifact.lower() == "local":
        try:
            save_csv_artifact(
                config=config,
                client_id=client_id,
                local_path=temp_csv,
                file_path=final_path,
                store_artifact="local"
            )
            # print(f"{final_path}")
        except Exception as e:
            raise Exception(f"Error saving CSV locally: {e}")

    # ------------------------------
    #  Cleanup
    # ------------------------------
    try:
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
    except Exception as e:
        print(f"[WARN] Failed to remove temporary CSV: {e}")
