from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import batch_process, log_process, authenticate, fetch_items_from_storage
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime
import requests
import time

semaphore = asyncio.Semaphore(5)  # Control the number of simultaneous requests

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
    table_management = settings.TABLE_MANAGEMENT
    destiny_table = settings.TABLE_VISITS

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')
    
    # Fetch item IDs from the storage bucket
    blob_items_prefix = f'{store_name}/meli/api_response/items/date={today_str}/'
    items_id = fetch_items_from_storage(
    storage, 
    bucket_name, 
    blob_items_prefix, 
    key_names='results'
    )

    print(f'** Items found: {len(items_id)}**')

    print(f'** Cleaning blob **')
    # Path for saving 
    blob_basic_path = settings.BLOB_VISITS(store_name)
    date_blob_path = f'{blob_basic_path}date={today_str}/'

    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path)

    print(f'** Starting API requests for {len(items_id)} items**')
    # URL function for API
    url = settings.URL_VISITS

    headers = {'Authorization': f'Bearer {access_token}'}

    # Check if it is the first time processing visits
    bool_not_first_time = storage.blob_exists(bucket_name, blob_basic_path)
    
    if not bool_not_first_time:
        print('** FIRST time processing visits **')
        num_days=150
    else:
        print('** NOT THE FIRST time processing visits **')
        num_days=1

    # # ------ ASSYNC TA ESTOURANDO O LIMITE DE REQUISIÇÕES ------
    # Batch processing the API requests
    # async with aiohttp.ClientSession() as session:
    #     await batch_process(session, items_id, url, headers, 
                            # bucket_name, date_blob_path, storage )

    my_responses = []
    batch_number = 0
    for i, item in enumerate(items_id):

        if (i + 1) % 50 == 0:
            print('Pause')
            time.sleep(7)

        if (i + 1) % 100 == 0:
            # Storing batch
            process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f-03:00")
            filename = f'batch_{batch_number}__process_time={process_time}.json'
            storage.upload_json(bucket_name, f'{date_blob_path}{filename}', my_responses)
            print(f'Saving file in storage: {filename}')
            batch_number +=1
            my_responses = []

        response = requests.get(url(item,num_days), headers=headers, params=item)
        data = response.json()
        data['item_id'] = item
        my_responses.append(data)

    print('** Logging process in management table... **')
    # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)

    return ('Success', 200)

def fetch_visits_data(request):
    return asyncio.run(main_async(request))
