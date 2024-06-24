import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import DiagnosticSettingsResource, LogSettings
import requests


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
        # If the request failed, log an error message
        logging.error(f"Failed to retrieve supported categories. Status code: {response.status_code}")
        return []


def main(event: func.EventGridEvent):
    logging.info('Python EventGrid trigger function processed an event.')

    # Parse the event data to extract the resource ID and type
    event_data = event.get_json()
    logging.info(f"Event data: {event_data}")  # Log the entire event data

    resource_uri = event_data.get('resourceUri')
    logging.info(f"Resource ID: {resource_uri}")  # Log the resource ID

    resource_type = event_data.get('resourceType')
    logging.info(f"Resource type: {resource_type}")  # Log the resource type

    subscription_id = "49abc43b-2948-4124-b421-784b6812e9c5"

    # Set up Azure credentials
    credential = DefaultAzureCredential()

    # Create the Monitor Management Client
    monitor_client = MonitorManagementClient(credential, subscription_id)

    # Retrieve supported log categories for the resource type
    access_token = get_access_token()
    if access_token:
        supported_log_categories = get_supported_log_categories(resource_uri, access_token)
        logging.info(f"Supported log categories: {supported_log_categories}")

        # Set the destination Storage Account ID
        storage_account_id = "/subscriptions/49abc43b-2948-4124-b421-784b6812e9c5/resourceGroups/dummy-rg/providers/Microsoft.Storage/storageAccounts/dummy01storage"

        # Define diagnostic settings for each supported log category

        log_settings = [LogSettings(enabled=True, category=category) for category in supported_log_categories]
        diagnostic_settings = DiagnosticSettingsResource(
            logs=log_settings,
            storage_account_id=storage_account_id
        )

        # Set a name for the diagnostic setting
        diagnostic_setting_name = f"diag-setting-{resource_uri.replace('/', '-')}"
        monitor_client.diagnostic_settings.create_or_update(
            resource_uri=resource_uri,
            name=diagnostic_setting_name,
            parameters=diagnostic_settings
        )

        logging.info(f"Diagnostic settings enabled for {resource_type}: {resource_uri}")
    else:
        logging.error("Failed to acquire access token.")
