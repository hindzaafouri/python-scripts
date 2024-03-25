import requests
import json
from utils.azure_auth import auth_vars


blob_service_client = auth_vars()
container_name_to_check = "insights-logs-auditlogs"

# Check if the specified container exists
container_client = blob_service_client.get_container_client(container_name_to_check)
blob_list = container_client.list_blobs()

for blob in blob_list:
    blob_client = container_client.get_blob_client(blob.name)
    blob_content = blob_client.download_blob().readall().decode('utf-8').splitlines()

    print(f"Content of blob {blob.name}:")

    for line in blob_content:
        try:
            json_data = json.loads(line)
            print(json_data)

            # Use the Logstash module to send data to Logstash
            #send_to_logstash(json_data)
        except Exception as e:
            print(f"Error processing blob: {e}")

        # except json.decoder.JSONDecodeError as e:
        #     print(f"Error decoding JSON: {e}")
        # except Exception as e:
        #     print(f"Error sending log to Logstash: {e}")

    print()
