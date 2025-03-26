import requests

def fetch_paginated_data(start_url, headers):
        """
        Iteratively fetch paginated data using 'next' links.
        Returns the accumulated data in bytes.
        """
        data = b""  # Initialize as bytes
        next_url = start_url

        while next_url:  # Continue until there are no more pages
            response = requests.get(next_url, headers=headers)
            response.raise_for_status()
            data += response.content  # Append fetched content

            # Extract 'next' link if available
            links = response.json().get("links", [])
            next_links = [link["href"] for link in links if link.get("rel") == "next"]
            next_url = next_links[0] if next_links else None  # Move to the next page or stop

        if not data:
            raise Exception("No data retrieved during pagination.")

        return data