from azure.storage.blob import BlobServiceClient

# Specify your Azure Storage account details
storage_account_name = "dummy01storage"
storage_account_key = "E+zzBuRGMoAKDqXfG6HZznlTgWJhAZFJHBJRM06NejuXA7lcZ/hhIIeLuciTjnY0vDUAm4E9StG0+AStcaCjWQ=="

# Create a BlobServiceClient using account name and key
blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)

# Specify the container and year path to check
container_name_to_check = "insights-logs-errors"

# Check if the specified container exists
container_client = blob_service_client.get_container_client(container_name_to_check)
blob_list = container_client.list_blobs()

# Iterate through each blob and print its raw content as bytes
for blob in blob_list:
    blob_client = container_client.get_blob_client(blob.name)
    blob_content = blob_client.download_blob().readall()

    print(f"Content of blob {blob.name}:")

    try:
        # Process the bytes data as needed
        # For example, you might want to save the binary data to a file or analyze it in some way.
        # Avoid trying to decode binary data as UTF-8, as it's not valid for non-text binary formats.
        print(blob_content)
    except Exception as e:
        print(f"Error processing blob: {e}")
