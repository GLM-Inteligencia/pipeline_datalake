from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import batch_process, log_process, authenticate, fetch_items_from_storage
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
    table_management = settings.TABLE_MANAGEMENT
    destiny_table = settings.TABLE_COMPETITORS_PRICES

    # competitors input table
    table_competitors_input = settings.TABLE_INPUT_COMPETITORS

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')
    
    # Fetch item IDs from the input bigquery
    query = f'''
    select distinct competitor_id
    from {table_competitors_input}
    where reference_seller_id = {seller_id}
    '''

    df_competitors = bigquery.run_query(query)
    items_id = df_competitors['competitor_id'].to_list()
    print(f'** Items found: {len(items_id)}**')

    print(f'** Cleaning blob **')
    # Path for saving price details
    blob_basic_path = settings.BLOB_COMPETITORS_PRICES(store_name)
    date_blob_path = f'{blob_basic_path}date={today_str}/'

    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path)

    print(f'** Starting API requests for {len(items_id)} items**')
    # URL function for API
    if len(items_id) == 0:
        print("** No items to process **")
        return ('Success', 200)

    url = settings.URL_PRICE_MARKETPLACE
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Batch processing the API requests
    async with aiohttp.ClientSession() as session:
        await batch_process(session, items_id, url, headers, bucket_name, date_blob_path, storage, add_item_id = True)

    print('** Logging process in management table... **')
    # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)

    return ('Success', 200)

def fetch_competitors_prices_data(request):
    return asyncio.run(main_async(request))

