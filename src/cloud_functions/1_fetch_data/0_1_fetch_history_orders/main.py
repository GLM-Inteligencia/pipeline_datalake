import aiohttp
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import json
import aiofiles
from concurrent.futures import ThreadPoolExecutor
from flask import escape
import requests
import gc  # Garbage collection to free memory
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.utils import log_process, authenticate
from src.config import settings

executor = ThreadPoolExecutor(max_workers=10)
limit = 50
timezone_offset = "-03:00"

# Main entry point for the Cloud Function
def fetch_historic_orders(request):
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

    start_date = datetime.today() - timedelta(days=365)
    end_date = datetime.today() 

    blob_name = settings.BLOB_ORDERS(store_name)

    asyncio.run(main(start_date, end_date, access_token, seller_id, bucket_name, blob_name, storage))
    
    # uploading log table

    # delete data from management table
    print('** Deleting past logs in management table **')
    query = f"delete from {table_management} where seller_id = {seller_id} and table_name = '{destiny_table}'"
    bigquery.run_query(query)

    # insert data
    date_range = pd.date_range(start=start_date, end=end_date)
    df_log = pd.DataFrame({
        'seller_id' : [seller_id] * len(date_range),
        'table_name' : [destiny_table] * len(date_range),
        'process_date': date_range,
        'processed_to_bq': [False] * len(date_range),
        'last_bq_processing' : [None] * len(date_range)
    })
    print('** Inserting logs in management table **')
    bigquery.insert_dataframe(df_log, table_management)

    return f"Order data fetched from {start_date} to {end_date}"

# Main function to handle async process with semaphore
async def main(start_date, end_date, access_token, seller_id, bucket_name, folder_name, storage):
    date_range = pd.date_range(start_date, end_date)
    semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent requests

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(date_range), 15):
            tasks = [process_batch_with_semaphore(session, date, access_token, seller_id, bucket_name, semaphore, folder_name, storage) for date in date_range[i:i+15]]
            await asyncio.gather(*tasks)
            gc.collect()  # Explicitly call garbage collection after each batch to free memory

# Function to process data for a single date with semaphore
async def process_batch_with_semaphore(session, date, access_token, seller_id, bucket_name, semaphore, folder_name, storage):
    async with semaphore:  # Ensure that only a limited number of requests run concurrently
        await process_batch(session, date, access_token, seller_id, bucket_name, folder_name, storage)

# Function to process data for a single date, handling pagination
async def process_batch(session, date, access_token, seller_id, bucket_name, folder_name, storage):
    offset = 0
    responses = []
    sales = []

    while True:
        response = await fetch_data(session, date, offset, access_token, seller_id)
        if response and len(response['results']) > 0:
            responses.append(response)
            sales.append(len(response['results']))
            offset += limit
        else:
            break

    if responses:
        await save_to_gcs_async(date, sum(sales), responses, bucket_name, folder_name, seller_id, storage)

# Function to fetch data for a given date and offset
async def fetch_data(session, date, offset, access_token, seller_id):
    formatted_date_start = date.strftime(f"%Y-%m-%dT00:00:00.000{timezone_offset}")
    formatted_date_end = (date + timedelta(days=1)).strftime(f"%Y-%m-%dT00:00:00.000{timezone_offset}")
    params = {
        'limit': limit,
        'offset': offset,
        'order.date_created.from': formatted_date_start,
        'order.date_created.to': formatted_date_end
    }
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    url = settings.URL_ORDERS(seller_id)

    async with session.get(url, params=params, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        else:
            error_message = await response.text()
            print(f"Error: {response.status}, Message: {error_message}")
            return None


# Function to asynchronously save data to GCS
async def save_to_gcs_async(date, num_sales, data, bucket_name, folder_name, seller_id, storage):
    date_str = date.strftime('%Y-%m-%d')
    timezone_offset = "-03:00" # Brazilian time
    process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f{timezone_offset}")
    file_name = f"{folder_name}date={date_str}/total_sales={num_sales}__data={date_str}__processing-time={process_time}.json"
    json_data = json.dumps(data, indent=4)

    temp_file_path = f'/tmp/orders_{date_str}.json'
    async with aiofiles.open(temp_file_path, 'w') as f:
        await f.write(json_data)


    await asyncio.get_running_loop().run_in_executor(executor, upload_to_gcs, temp_file_path, file_name, bucket_name, storage)

# Function to upload the file to GCS and insert into BigQuery
def upload_to_gcs(temp_file_path, file_name, bucket_name, storage):
    # Upload file to GCS using storage.client
    bucket = storage.client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(temp_file_path)
    print(f"Uploaded {file_name} to GCS using storage.client")
