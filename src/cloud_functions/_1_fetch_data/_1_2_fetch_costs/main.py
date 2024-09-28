from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import batch_process, log_process, authenticate, fetch_items_from_storage
from src.config import settings
import json
import asyncio
import aiohttp
from datetime import datetime
import requests

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
    destiny_table = settings.TABLE_COSTS

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Getting params to see costs
    query = f'''
        with items_details as (
        select distinct
            item_id,
            listing_type,
            category_id
        from datalake-v2-424516.datalake_v2.items_details
        where
            1=1
            and date(correspondent_date) = current_date()
            and seller_id = {seller_id}
        )

        select 
        p.item_id as id,
        d.listing_type as listing_type_id,
        d.category_id,
        p.price,
        p.channel
        from datalake-v2-424516.datalake_v2.items_prices p
        inner join items_details d
        on p.item_id = d.item_id
        where 
            1=1
            and date(p.correspondent_date) = current_date()
            and channel is not null
    '''
    # blob_items_prefix = f'{store_name}/meli/api_response/item_detail/date={today_str}/'
    # items_id = fetch_items_from_storage(
    # storage, 
    # bucket_name, 
    # blob_items_prefix, 
    # key_names=['id','price', 'category_id', 'listing_type_id']
    # )

    df_params = bigquery.run_query(query)
    # items = df_params[['id','channel']].to_dict(orient='records')
    df_params['channel'] = df_params['channel'].apply(lambda x : x.replace('channel_', '')).drop(columns = 'channel')
    items_id = df_params.to_dict(orient='records')

    print(f'** Items found: {len(items_id)}**')

    print(f'** Cleaning blob **')
    # Path for saving 
    blob_basic_path = settings.BLOB_COSTS(store_name)
    date_blob_path = f'{blob_basic_path}date={today_str}/'

    # Clean existing files in the storage bucket
    storage.clean_blobs(bucket_name, date_blob_path)

    print(f'** Starting API requests for {len(items_id)} items**')
    # URL function for API
    url = settings.URL_COST
    headers = {'Authorization': f'Bearer {access_token}'}

    # ------------ ASSYNC COM PROBLEMA NA CONCATENACAO DO ID ----------------
    # # Batch processing the API requests
    # async with aiohttp.ClientSession() as session:
    #     await batch_process(session, items_id, url, headers, 
    #                         bucket_name, date_blob_path, storage, 
    #                         params = items_id, add_item_id = True)

    my_responses = []
    batch_number = 0
    for i, item in enumerate(items_id):
        if (i + 1) % 100 == 0:
            # Storing batch
            process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f-03:00")
            filename = f'batch_{batch_number}__process_time={process_time}.json'
            storage.upload_json(bucket_name, f'{date_blob_path}{filename}', my_responses)
            print(f'Saving file in storage: {filename}')
            batch_number +=1
            my_responses = []

        response = requests.get(url, headers=headers, params=item)
        data = response.json()
        data['item_id'] = item
        my_responses.append(data)

    print('** Logging process in management table... **')
    # Log the process in BigQuery
    log_process(seller_id, destiny_table, today_str, table_management, processed_to_bq=False)

    return ('Success', 200)

def fetch_costs_data(request):
    return asyncio.run(main_async(request))

