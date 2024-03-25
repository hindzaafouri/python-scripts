from azure.storage.blob import BlobServiceClient

def auth_vars () :
    storage_account_name = "dummy01storage"
    storage_account_key = "E+zzBuRGMoAKDqXfG6HZznlTgWJhAZFJHBJRM06NejuXA7lcZ/hhIIeLuciTjnY0vDUAm4E9StG0+AStcaCjWQ=="
    # Create a BlobServiceClient using account name and key
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
    return blob_service_client
