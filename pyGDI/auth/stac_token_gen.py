import requests

class StacTokenGenerator:
    """
    A class to generate authentication tokens using client credentials.

    Attributes:
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.
        token_url (str): The URL to fetch the token from.
        item_id (str): The item ID for the request payload.
        item_type (str): The item type for the request payload.
        role (str): The role for the request payload.
    """

    def __init__(self, client_id: str, client_secret: str, role: str, collection_id:str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url =  "https://dx.geospatial.org.in/auth/v1/token"
        self._item_id = collection_id
        self._item_type = "resource"
        self._role = role

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def client_secret(self) -> str:
        return self._client_secret
    
    @property
    def role(self):
        return self._role
    
    @property
    def collection_id(self):
        return self._item_id

    @property
    def get_stac_token(self) -> str:
        """
        Generate an authentication token.

        :return: The generated token as a string.
        :raises Exception: If the token request fails.
        """
        try:
            response = requests.post(
                   self._token_url,
                headers={
                    "clientId": self._client_id,
                    "clientSecret": self._client_secret,
                    "Content-Type": "application/json"                    
                },
                json={
                    "itemId": self._item_id,
                    "itemType": self._item_type,
                    "role": self._role
                }
            )
            
            

            response.raise_for_status()  # Raise an HTTPError for bad responses
            # print(response.json())
            token = response.json().get('results', {}).get('accessToken')
            if not token:
                raise Exception("Failed to retrieve access token from response.")
            return token
        except requests.RequestException as e:
            # print(response.json())
            raise Exception(f"Error generating auth token: {e}")
        

def createSTACAuth(client_id: str, client_secret: str, role: str, collection_id:str) -> StacTokenGenerator:

    tokengenerator= StacTokenGenerator(client_id=client_id, client_secret=client_secret, role=role, collection_id=collection_id)
    return tokengenerator



# Example usage
# if __name__ == "__main__":
#     client_id = "7dcf1193-4237-48a7-a5f2-4b530b69b1cb"
#     client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
#     role = "consumer"

#     try:
#         token_generator = StacTokenGenerator(client_id, client_secret, role, '28e16f74-0ff8-4f11-a509-60fe078d8d47')
#         auth_token = token_generator.generate_token()
#         print("Generated Auth Token:", auth_token)
#     except Exception as e:
#         print(e)
