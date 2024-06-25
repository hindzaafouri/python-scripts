import logging
import pyodbc
import pandas as pd
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import os
import struct

def connect_to_sql_server(server, database):
    try:
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token = credential.get_token("https://database.windows.net/.default")
        token_bytes = token.token.encode("utf-16-le")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{server}.database.windows.net,1433;"
            f"Database={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )

        logging.info(f"Connection string: {conn_str}")

        conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        logging.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Azure SQL Database: {e}")
        raise

def fetch_audit_logs(conn, blob_url):
    cursor = conn.cursor()
    
    logging.info(f"Blob URL: {blob_url}")
    
    query = f"""
    SELECT event_time, client_ip, application_name AS application, duration_milliseconds AS duration,
           host_name AS hostname, action_id, succeeded, session_server_principal_name,
           database_name, statement
    FROM sys.fn_get_audit_file(
        N'{blob_url}',
        DEFAULT,
        DEFAULT
    )
    WHERE object_name IS NOT NULL  -- Ensure only valid audit log entries
      AND statement IS NOT NULL    -- Ensure statement is not NULL to exclude unexpected data
      AND statement <> ''          -- Additional check if statement might be an empty string instead of null
      AND statement NOT LIKE '%sys.fn_get_audit_file%'  -- Exclude logs where statement contains sys.fn_get_audit_file
      AND statement NOT LIKE '%sp_datatype_info_100%'
      AND statement NOT LIKE '%FROM sys.tables%'
      AND statement NOT LIKE '%FROM sys.columns%'
      AND statement NOT LIKE '%FROM sys.indexes%'
    """
    
    logging.info(f"Executing query: {query}")
    
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        logging.info(f"Number of rows fetched: {len(rows)}")
    except Exception as e:
        logging.error(f"Error fetching audit logs: {e}")
        rows = []
    finally:
        cursor.close()

    # Convert rows to a list of tuples
    data = [tuple(row) for row in rows]

    return data

def export_to_csv(data, csv_file):
    columns = [
        'event_time', 'client_ip', 'application', 'duration', 'hostname', 
        'action_id', 'succeeded', 'session_server_principal_name', 
        'database_name', 'statement'
    ]
    
    df = pd.DataFrame(data, columns=columns)
    
    df.to_csv(csv_file, index=False)
    logging.info(f"Data exported to {csv_file}")

def csv_to_json(csv_file):
    df = pd.read_csv(csv_file)
    
    json_data = df.to_json(orient='records', lines=True)
    
    logging.info(f"JSON data:\n{json_data}")

    return json_data

def upload_to_blob(json_data, blob_name):
    try:
        storage_account_name = "dummy01storage"
        
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)
        
        container_name = "sqldbauditlogs-json"
        container_client = blob_service_client.get_container_client(container_name)
        
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(json_data, overwrite=True)
        
        logging.info(f"JSON data uploaded to blob: {blob_name}")
    except Exception as e:
        logging.error(f"Error uploading JSON data to blob: {e}")

def main(blob: func.InputStream):
    try:
        blob_path = blob.name
        logging.info(f"Processing blob: {blob_path}")

        parts = blob_path.split('/')
        
        if len(parts) < 6:
            logging.error("Invalid blob path format.")
            return
        
        storage_account_name = "dummy01storage" 
        container_name = parts[0]  # sqldbauditlogs
        server = parts[1]   # server-firsttest
        database = parts[2]  # db03
        folder_path = '/'.join(parts[3:5])  # SqlDbAuditing_Audit_NoRetention/2024-06-25
        
        blob_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{server}/{database}/{folder_path}"
        
        conn = connect_to_sql_server(server, database)
        
        audit_logs = fetch_audit_logs(conn, blob_url)
        
        if audit_logs:
            csv_file = f'{server}_{database}_audit_logs.csv'
            
            #export_to_csv(audit_logs, csv_file)

            json_data = csv_to_json(csv_file)
            
            logging.info(f"JSON data:\n{json_data}")

            xel_file_name = parts[-1]
            json_file_name = xel_file_name.replace('.xel', '.json')
            
            blob_name = f"{json_file_name}"
            upload_to_blob(json_data, blob_name)

        else:
            logging.warning(f"No audit logs found in {folder_path}")
        
        if conn:
            conn.close()
    except Exception as e:
        logging.error(f"Error processing blob: {e}")
