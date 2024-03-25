import logging
from azure.functions import InputStream
import time

# batch size
BATCH_SIZE = 2

current_batch = []
last_processing_time = time.time()

def process_batch(batch):
    logging.info("Processing batch...")
    for blob in batch:
        logging.info(f"Blob Name: {blob.name}")
        logging.info(f"Blob Content:")
        content = blob.read().decode('utf-8')

        logging.info(content)
    logging.info("Batch processing complete.")

def main(myblob: InputStream):
    global current_batch, last_processing_time
    
    # Add the incoming blob to the current batch
    current_batch.append(myblob)
    
    if len(current_batch) == BATCH_SIZE:
        process_batch(current_batch)
        
        # Reset the current batch and update the last processing time
        current_batch = []
        last_processing_time = time.time()
