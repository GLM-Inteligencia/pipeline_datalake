from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import authenticate, fetch_items
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime

semaphore = asyncio.Semaphore(100)  # Control the number of simultaneous requests

async def main_async(request):
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

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    print(f'** Cleaning blob **')
    # Path for saving 
    blob_basic_path = settings.BLOB_ITEMS(store_name)
    date_blob_path = f'{blob_basic_path}date={today_str}/'

    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path)

    print(f'** Starting API requests **')
    # URL function for API
    url = settings.URL_ITEMS
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Fetch and save all items
    all_products, all_responses = fetch_items(url, access_token, seller_id)

    # creating file path
    timezone_offset = "-03:00" # Brazilian time
    process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f{timezone_offset}")
    file_name = f'total_items={len(all_products)}__processing-date={process_time}.json'
    file_path = date_blob_path + file_name 
    storage.upload_json(bucket_name, file_path, all_responses)
    
    return ('Success', 200)

def fetch_items_data(request):
    return asyncio.run(main_async(request))

