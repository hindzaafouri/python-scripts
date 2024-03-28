from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import DiagnosticSettingsResource, LogSettings
from azure.mgmt.resource.resources import ResourceManagementClient

# Set Azure subscription ID and resource types
subscription_id = "49abc43b-2948-4124-b421-784b6812e9c5"
resource_types = ["Microsoft.Sql/servers/databases"]

# Set the destination Storage Account ID
storage_account_id = "/subscriptions/49abc43b-2948-4124-b421-784b6812e9c5/resourceGroups/dummy-rg/providers/Microsoft.Storage/storageAccounts/dummy01storage"
# Set up Azure credentials
credential = DefaultAzureCredential()

# Create the Resource Management Client
resource_client = ResourceManagementClient(credential, subscription_id)

# Create the Monitor Management Client
monitor_client = MonitorManagementClient(credential, subscription_id)

# Define function to enable diagnostic settings
def enable_diagnostic_settings(resource_id):
    # Check if diagnostic settings already exist
    diagnostic_settings = monitor_client.diagnostic_settings.list(resource_uri=resource_id)
    if not any(diagnostic_settings):
        # Define diagnostic settings for error logs with the specified storage account ID
        log_settings = [LogSettings(enabled=True, category='Errors')]
        diagnostic_settings = DiagnosticSettingsResource(
            logs=log_settings,
            storage_account_id=storage_account_id
        )
        # Set a name for the diagnostic setting
        diagnostic_setting_name = f"diag-setting-{resource_id.replace('/', '-')}"
        monitor_client.diagnostic_settings.create_or_update(
            resource_uri=resource_id,
            name=diagnostic_setting_name,
            parameters=diagnostic_settings
        )
        print(f"Diagnostic settings enabled for resource: {resource_id}")
    else:
        print(f"Diagnostic settings already exist for resource: {resource_id}")


# Get resources and enable diagnostic settings
for resource_type in resource_types:
    resources = resource_client.resources.list(filter=f"resourceType eq '{resource_type}'")
    for resource in resources:
        enable_diagnostic_settings(resource.id)
