import requests

class TokenGenerator:
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

    def __init__(self, client_id: str, client_secret: str, role: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url =  "https://dx.gsx.org.in/auth/v1/token"
        self.item_id = "geoserver.dx.ugix.org.in"
        self.item_type = "resource_server"
        self.role = role

    def generate_token(self) -> str:
        """
        Generate an authentication token.

        :return: The generated token as a string.
        :raises Exception: If the token request fails.
        """
        try:
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
            print("Response Status Code:", response.status_code)
            

            response.raise_for_status()  # Raise an HTTPError for bad responses
            token = response.json().get('results', {}).get('accessToken')
            if not token:
                raise Exception("Failed to retrieve access token from response.")
            return token
        except requests.RequestException as e:
            raise Exception(f"Error generating auth token: {e}")


# # Example usage
# if __name__ == "__main__":
#     client_id = "7dcf1193-4237-48a7-a5f2-4b530b69b1cb"
#     client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
#     token_url = "https://dx.gsx.org.in/auth/v1/token"
#     item_id = "geoserver.dx.ugix.org.in"
#     item_type = "resource_server"
#     role = "consumer"

#     try:
#         token_generator = TokenGenerator(client_id, client_secret, token_url, item_id, item_type, role)
#         auth_token = token_generator.generate_token()
#         print("Generated Auth Token:", auth_token)
#     except Exception as e:
#         print(e)
