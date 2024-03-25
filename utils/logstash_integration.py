import requests
import json
from dotenv import load_dotenv
import os
load_dotenv()

LOGSTASH_HOST = os.getenv("LOGSTASH_HOST")
LOGSTASH_PORT = int(os.getenv("LOGSTASH_PORT"))

def send_to_logstash(json_data):
    try:
        # Send the parsed JSON data to Logstash using HTTP POST
        response = requests.post(f"http://{LOGSTASH_HOST}:{LOGSTASH_PORT}", json=json_data)

        # Check if the request was successful
        if response.status_code == 200:
            print("Log sent to Logstash successfully.")
        else:
            print(f"Error sending log to Logstash. Status code: {response.status_code}")

    except Exception as e:
        print(f"Error sending log to Logstash: {e}")