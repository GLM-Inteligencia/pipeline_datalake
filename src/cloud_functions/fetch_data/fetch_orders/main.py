from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import authenticate, fetch_sales_for_day, log_process
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime

def fetch_orders_data(request):
    # Parsing request data
    data = request.get_json()
    client_id = data.get('client_id')
    client_secret = data.get('client_secret')
    store_name = data.get('store_name')
    seller_id = data.get('seller_id')
    access_token = data.get('access_token')

    print('** Defining authentication... **')
    # Authenticate (assuming this is now centralized in utils.py or a similar file)
    if not access_token:
        access_token = authenticate(client_id, client_secret)  # You can add this to a common module

    print('** Connecting to storage and BigQuery... **')
    # Initialize storage and BigQuery
    storage = CloudStorage(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)

    # Define paths and table names from the config
    bucket_name = settings.BUCKET_STORES
    table_management = settings.TABLE_MANAGEMENT
    destiny_table = settings.TABLE_ORDERS

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    print(f'** Cleaning blob **')
    # Path for saving 
    blob_basic_path = settings.BLOB_ORDERS(store_name)
    date_blob_path = f'{blob_basic_path}date={today_str}/'
    destiny_table = settings.TABLE_ORDERS

    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path)

    print(f'** Starting API requests **')
    # URL function for API
    url = settings.URL_ORDERS
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Fetch and save all items
    num_sales, all_responses = fetch_sales_for_day(url(seller_id), access_token)

    # creating file path
    timezone_offset = "-03:00" # Brazilian time
    process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f{timezone_offset}")
    file_name = f'total_sales={num_sales}__processing-date={process_time}.json'
    file_path = date_blob_path + file_name 

    # saving files
    storage.upload_json(bucket_name, file_path, all_responses)

    # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)
    
    return ('Success', 200)
