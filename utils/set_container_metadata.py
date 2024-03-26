import os
from azure.storage.blob import BlobServiceClient, ContainerClient 

def set_container_metadata(container_name, metadata):
    # Read the Azure Storage account connection string from the environment variables
    connection_string = os.getenv("dummy01storage_STORAGE")

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a ContainerClient for the specified container
    container_client = blob_service_client.get_container_client(container_name)

    # Set the container metadata
    container_client.set_container_metadata(metadata)

    # Print the container metadata
    print("Container Metadata after setting:")
    print(metadata)


def is_container_initialized(container_name):
    # Read the Azure Storage account connection string from the environment variables
    connection_string = os.getenv("dummy01storage_STORAGE")

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a ContainerClient for the specified container
    container_client = blob_service_client.get_container_client(container_name)

    # Retrieve the container properties
    container_properties = container_client.get_container_properties()

    # Check if the "Initialized" metadata exists and its value is "True"
    metadata = container_properties.metadata
    if metadata and metadata.get("Initialized") == "True":
        return True
    else:
        return False