import requests
from token_gen import TokenGenerator

class ResourceFetcher:
    def __init__(self, client_id: str, client_secret: str, token_url: str):
        """
        Initialize the ResourceFetcher with authentication details.

        :param client_id: The client ID for authentication.
        :param client_secret: The client secret for authentication.
        :param token_url: The URL to fetch the token from.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url

    def fetch_resource_data(self, item_id: str, item_type: str, role: str, resource_id: str, offset: int = 1) -> dict:
        """
        Fetch data for a specified resource using the generated token.

        :param item_id: The item ID for the token payload.
        :param item_type: The item type for the token payload.
        :param role: The role for the token payload.
        :param resource_id: The resource ID to fetch data for.
        :param offset: The offset parameter for the resource query (default is 1).
        :return: The fetched resource data as a dictionary.
        :raises Exception: If the request for resource data fails.
        """
        try:
            # Generate the token
            token_generator = TokenGenerator(self.client_id, self.client_secret, self.token_url, item_id, item_type, role)
            auth_token = token_generator.generate_token()

            # Fetch resource data
            resource_url = f"https://geoserver.dx.ugix.org.in/collections/{resource_id}/items?offset={offset}"
            headers = {"Authorization": f"Bearer {auth_token}"}

            response = requests.get(resource_url, headers=headers)
            response.raise_for_status()  # Raise an HTTPError for bad responses
            print("Response on the way")
            return response.json()  # Return the fetched data as a dictionary
        except requests.RequestException as e:
            raise Exception(f"Error fetching resource data: {e}")


if __name__ == "__main__":
    client_id = "7dcf1193-4237-48a7-a5f2-4b530b69b1cb"
    client_secret = "a863cafce5bd3d1bd302ab079242790d18cec974"
    token_url = "https://dx.ugix.org.in/auth/v1/token"
    item_id = "geoserver.dx.ugix.org.in"
    item_type = "resource_server"
    role = "consumer"
    resource_id = "95886a22-704d-4922-815c-39af80acd520"
    offset = 1

    fetcher = ResourceFetcher(client_id, client_secret, token_url)
    try:
        resource_data = fetcher.fetch_resource_data(item_id, item_type, role, resource_id, offset=1)
        print("Fetched Resource Data:", resource_data)
    except Exception as e:
        print(e)
