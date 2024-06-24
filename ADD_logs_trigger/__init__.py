import logging
from azure.functions import InputStream
from azure.storage.blob import BlobServiceClient
from utils.set_container_metadata import set_container_metadata, is_container_initialized
import os
import time

# Batch size
BATCH_SIZE = 3

current_batch = []
last_processing_time = time.time()

def process_batch(batch):
    # Function to process the current batch
    logging.info("Processing batch...")
    for blob in batch:
        blob_content = blob.read().decode('utf-8')
        logging.info(f"Blob Name: {blob.name}")
        logging.info(f"Blob Content:")
        logging.info(blob_content)
    logging.info("Batch processing complete.")

def process_all_blobs(container_client):
    logging.info("Processing all blobs in the container...")
    blobs = container_client.list_blobs()
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        logging.info(f"Blob Name: {blob.name}")
        logging.info(f"Blob Content:")
        content = blob_client.download_blob().readall().decode('utf-8')
        logging.info(content)
    logging.info("All blobs processed.")

def main(myblob: InputStream):
    global current_batch, last_processing_time
    
    container_name = "insights-logs-auditlogs"
    current_batch.append(myblob)

    if not is_container_initialized(container_name):
        connection_string = os.getenv("dummy01storage_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        metadata = {"Initialized": "True"}
        set_container_metadata(container_name, metadata)
        process_all_blobs(container_client)

    logging.info(f" ++++++++++++++++++++++++++++ Current batch size: {len(current_batch)}")

    # If initialized, check batch size and process batch if BATCH_SIZE is reached
    if len(current_batch) == BATCH_SIZE:
        process_batch(current_batch)
        # Reset the current batch and update the last processing time
        current_batch = []
        last_processing_time = time.time()
