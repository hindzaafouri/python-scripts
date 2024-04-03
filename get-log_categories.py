import os
import requests
from azure.identity import DefaultAzureCredential

def get_access_token():
    # Use DefaultAzureCredential to acquire a token
    credential = DefaultAzureCredential()
    token = credential.get_token("https://management.azure.com/")
    return token.token

def get_supported_log_categories(resource_uri, access_token):
    # Construct the URL for the diagnosticSettingsCategories endpoint
    url = f"https://management.azure.com/{resource_uri}/providers/Microsoft.Insights/diagnosticSettingsCategories?api-version=2021-05-01-preview"

    # Make a GET request to the endpoint
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        # Extract the supported categories from the response
        supported_categories = [category["name"] for category in data["value"] if category["properties"]["categoryType"] == "Logs"]
        return supported_categories
    else:
        # If the request failed, print an error message
        print(f"Failed to retrieve supported categories. Status code: {response.status_code}")
        return None

# Example usage
resource_uri = "/subscriptions/49abc43b-2948-4124-b421-784b6812e9c5/resourcegroups/db-rg01/providers/microsoft.sql/servers/dummy-sqlserver/databases/db0078"
access_token = get_access_token()
if access_token:
    supported_log_categories = get_supported_log_categories(resource_uri, access_token)
    if supported_log_categories:
        print(f"Supported log categories: {supported_log_categories}")
