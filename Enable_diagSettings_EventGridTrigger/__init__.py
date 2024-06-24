import os
from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.monitor.models import DiagnosticSettingsResource, LogSettings
from azure.functions import EventGridEvent 
import logging
import json

def main(event: EventGridEvent):
    try:
        event_data = event.get_json()

        # Extract necessary information from the event payload
        resource_id = event_data['data']['resourceUri']
        resource_type = event_data['data']['resourceType']
        
        # Define your Azure subscription ID
        subscription_id = os.environ.get("subscription_id")
        
        # Set up Azure credentials
        credential = DefaultAzureCredential()

        # Create the Monitor Management Client
        monitor_client = MonitorManagementClient(credential, subscription_id)

        # Check if the resource type is SQL database
        if resource_type.lower() == 'microsoft.sql/servers/databases':
            # Define diagnostic settings for SQL database
            diagnostic_settings = DiagnosticSettingsResource(
                logs=get_logs_for_resource_type(resource_type)
            )

            # Enable diagnostic settings for the SQL database
            monitor_client.diagnostic_settings.create_or_update(
                resource_uri=resource_id,
                parameters=diagnostic_settings
            )

            logging.info('Diagnostic settings enabled for SQL database: %s', resource_id)
        else:
            logging.info('Skipping diagnostic settings enabling for resource type: %s', resource_type)
    
    except Exception as e:
        logging.error('Error occurred while enabling diagnostic settings: %s', str(e))

def get_logs_for_resource_type(resource_type):
    # Define logs to enable based on resource type
    logs = []
    if resource_type == 'Microsoft.Sql/servers/databases':
        logs = [LogSettings(enabled=True, category='SQLSecurityAuditEvents')]
    # Add more conditions for other resource types as needed
    return logs
