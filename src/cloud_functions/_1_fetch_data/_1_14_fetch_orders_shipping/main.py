from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import batch_process, fetch_shipping_ids_from_results, log_process, authenticate
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime, timedelta
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
    destiny_table = settings.TABLE_ORDERS_SHIPPING
    # Define today's date
    #today_str = datetime.today().strftime('%Y-%m-%d')
    today_str = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Fetch item IDs from the storage bucket
    blob_items_prefix = f'{store_name}/meli/api_response/orders/date={today_str}/'
    shipments_id = fetch_shipping_ids_from_results(storage, bucket_name, blob_items_prefix, filter_key=None, filter_value=None)
    print(f'** Shipments found: {len(shipments_id)}**')
    
    print(f'** Cleaning blob **')
    # # Path for orders shipping

    blob_basic_path_orders_shipping = settings.BLOB_ORDERS_SHIPPING_COST(store_name)
    date_blob_path_orders_shipping = f'{blob_basic_path_orders_shipping}date={today_str}/'
    
    
    # # # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path_orders_shipping)

    print(f'** Starting API requests for {len(shipments_id)} items**')
    # # URL function for API
    url_orders_shipping_cost = settings.URL_ORDERS_SHIPPING_COST
    headers = {'Authorization': f'Bearer {access_token}'}
    
    
    # # Batch processing the API requests
    async with aiohttp.ClientSession() as session:
        await batch_process(session, shipments_id, url_orders_shipping_cost, headers, bucket_name, date_blob_path_orders_shipping, storage, add_item_id = True, sleep_time=1)

    print('** Logging process in management table... **')
    # # # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)

    return ('Success', 200)

def fetch_orders_shipping_cost(request):
    return asyncio.run(main_async(request))

