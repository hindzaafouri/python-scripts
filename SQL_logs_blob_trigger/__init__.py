import logging
import pyodbc
import pandas as pd
import azure.functions as func
from azure.identity import DefaultAzureCredential
import os

def connect_to_sql_server(server, database):
    try:
        credential = DefaultAzureCredential()
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server=tcp:{server}.database.windows.net,1433;"
            f"Database={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        token = credential.get_token(f"https://{server}.database.windows.net")
        token_struct = struct.pack(f'<I{len(token.token.encode("utf-16-le"))}s', len(token.token.encode("utf-16-le")), token.token.encode("utf-16-le"))
        SQL_COPT_SS_ACCESS_TOKEN = 1256  
        conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Azure SQL Database: {e}")
        raise

def fetch_audit_logs(conn, storage_url):
    cursor = conn.cursor()
    
    query = f"""
    SELECT event_time, client_ip, application_name AS application, duration_milliseconds AS duration,
           host_name AS hostname, action_id, succeeded, session_server_principal_name,
           database_name, statement
    FROM sys.fn_get_audit_file(
        N'{storage_url}',
        DEFAULT,
        DEFAULT
    )
    WHERE object_name IS NOT NULL  -- Ensure only valid audit log entries
      AND statement IS NOT NULL;   -- Ensure statement is not NULL to exclude unexpected data
    """
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
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
    
    print(json_data)

    return json_data

def main(blob: func.InputStream):
    try:
        blob_path = blob.name
        logging.info(f"Processing blob: {blob_path}")

        parts = blob_path.split('/')
        
        # Ensure parts has enough elements to proceed
        if len(parts) < 5:
            logging.error("Invalid blob path format.")
            return
        
        # Extract components based on the provided example path structure
        storage_account_name = parts[0]  # sqldbauditlogs
        server = parts[1]   # server-firsttest
        database = parts[2]  # db03
        rest_of_path = '/'.join(parts[3:])  # SqlDbAuditing_Audit_NoRetention/2024-06-22/12_35_42_430_0.xel
        
        full_storage_url = f'https://{storage_account_name}.blob.core.windows.net/{server}/{database}/{rest_of_path}'
        
        logging.info(f"Full Blob Storage URL: {full_storage_url}")
        
        conn = connect_to_sql_server(server, database)
        
        audit_logs = fetch_audit_logs(conn, full_storage_url)
        
        if audit_logs:
            csv_file = f'{server}_{database}_audit_logs.csv'
            
            export_to_csv(audit_logs, csv_file)

            json_data = csv_to_json(csv_file)
            
            logging.info(f"JSON data:\n{json_data}")
        else:
            logging.warning(f"No audit logs found in {full_storage_url}")
        
        if conn:
            conn.close()
    except Exception as e:
        logging.error(f"Error processing blob: {e}")
