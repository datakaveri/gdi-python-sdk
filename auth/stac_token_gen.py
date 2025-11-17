import requests
import urllib
import json

def check_access_policy_in_catalogue(item_id):

    try:
        response = urllib.request.urlopen(
            f"https://dx.geospatial.org.in/dx/cat/v1/item?id={item_id}"
        )
        json_resp = json.load(response)

        if "results" not in json_resp or len(json_resp["results"]) == 0:
            raise Exception(f"Invalid response sent by DX catalogue server : {js}")

        item_info = json_resp["results"][0]

        if "accessPolicy" not in item_info:
            raise Exception(f"Invalid response sent by DX catalogue server : {js}")

        return item_info["accessPolicy"]

    except json.JSONDecodeError as e:
        raise Exception(
            f"Non JSON response sent from DX catalogue server : {e.doc}"
        )
    except urllib.error.HTTPError as e:
        raise Exception(
            f"Non 2xx status code received from DX catalogue server : {e.code}, {e.fp.read()}"
        )
    except urllib.error.URLError as e:
        raise Exception(e)

class StacTokenGenerator:
    """
    A class to generate authentication tokens using client credentials.

    Attributes:
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.
        token_url (str): The URL to f6443e79-0594-4d65-bb0d-d025893c99fefetch the token from.
        item_id (str): The item ID for the request payload.
        item_type (str): The item type for the request payload.
        role (str): The role for the request payload.
    """


    def __init__(self, client_id: str, client_secret: str, role: str, collection_id:list):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url =  "https://dx.geospatial.org.in/auth/v1/token"
        self.item_id = collection_id
        self.item_type = "resource_server"
        self.role = role

 
    
    def generate_token(self) -> str:
        """
        Generate an authentication token.

        :return: The generated token as a string.
        :raises Exception: If the token request fails.
        """
        try:
            access_policy = check_access_policy_in_catalogue(self.item_id)
            # print("Access Policy for this resource: ", access_policy)


            if access_policy == "OPEN":
                self.item_type = "resource_server"
                self.item_id = 'geoserver.dx.geospatial.org.in'

            if access_policy == "SECURE":
                self.item_type = "resource"

            response = requests.post(
                   self.token_url,
                headers={
                    "clientId": self.client_id,
                    "clientSecret": self.client_secret,
                    "Content-Type": "application/json"                    
                },
                json={
                    "itemId": self.item_id,
                    "itemType": self.item_type,
                    "role": self.role
                }
            )
            # print(self.item_type)
            # print(self.role)
            # print(self.item_id)
            # print('\n\n')
            # print(response.json())

            response.raise_for_status()
            token = response.json().get('results', {}).get('accessToken')
            # print(token)
            if not token:
                raise Exception("Failed to retrieve access token from response.")
            return token
        except Exception as e:
            raise Exception(f"Error generating auth token: {e}")


# # Example usage
# # if __name__ == "__main__":
# #     client_id = "7dcf1193-4237-48a7-a5f2-4b530b69b1cb"
# #     client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
# #     role = "consumer"

# #     try:
# #         token_generator = StacTokenGenerator(client_id, client_secret, role, '28e16f74-0ff8-4f11-a509-60fe078d8d47')
# #         auth_token = token_generator.generate_token()
# #         print("Generated Auth Token:", auth_token)
# #     except Exception as e:
# #         print(e)
