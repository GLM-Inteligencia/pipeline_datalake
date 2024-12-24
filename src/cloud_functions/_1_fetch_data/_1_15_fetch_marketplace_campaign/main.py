from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import batch_process, fetch_promotion_ids_from_storage, log_process, authenticate, fetch_items_from_storage
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime
import time

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
    #bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    
    # Define paths and table names from the config
    bucket_name = settings.BUCKET_STORES
    table_management = settings.TABLE_MANAGEMENT
    destiny_table = settings.TABLE_ITEM_PROMOTION
    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')
    
    # Fetch promotion IDs from the storage bucket
    blob_promotions_prefix = f'{store_name}/meli/api_response/items_promotions/date={today_str}/'
    #promotions_id = fetch_promotion_ids_from_storage(storage, settings.BUCKET_STORES, blob_promotions_prefix)
    promotions_id = fetch_promotion_ids_from_storage(storage, bucket_name, blob_promotions_prefix)
    print(f'** Promotions found: {len(promotions_id)}**')
    
    print(f'** Cleaning blob **')
    # Path for saving promotions details
    # marketplace
    blob_basic_path_marketplace = settings.BLOB_MARKETPLACE_CAMPAIGN(store_name)
    date_blob_path_marketplace = f'{blob_basic_path_marketplace}date={today_str}/'
    
    
    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path_marketplace)

    print(f'** Starting API requests for {len(promotions_id)} promotions**')
    # URL function for API
    url_marketplace = settings.URL_MARKETPLACE_CAMPAIGN
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Batch processing the API requests
    # PROMOTIONS MARKETPLACE_CAMPAIGN
    async with aiohttp.ClientSession() as session:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            await batch_process(
                session, 
                promotions_id, 
                url_marketplace, 
                headers, 
                bucket_name, 
                date_blob_path_marketplace, 
                storage, 
                add_item_id=False, 
                sleep_time=1
            )

    # item marketplace campaign
    blob_basic_path_item_marketplace = settings.BLOB_ITEM_MARKETPLACE_CAMPAIGN(store_name)
    date_blob_path_item_marketplace = f'{blob_basic_path_item_marketplace}date={today_str}/'

    #URL function for API
    url_item_marketplace = settings.URL_ITEM_MARKETPLACE_CAMPAIGN
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Batch processing the API requests
    # PROMOTIONS ITEMS MARKETPLACE_CAMPAIGN   
    async with aiohttp.ClientSession() as session:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
            await batch_process(
                session, 
                promotions_id, 
                url_item_marketplace, 
                headers, 
                bucket_name, 
                date_blob_path_item_marketplace, 
                storage, 
                add_item_id=True, 
                sleep_time=1
            )


    
    print('** Logging process in management table... **')
    # # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)

    return ('Success', 200)

def fetch_marketplace_campaign(request):
    return asyncio.run(main_async(request))

