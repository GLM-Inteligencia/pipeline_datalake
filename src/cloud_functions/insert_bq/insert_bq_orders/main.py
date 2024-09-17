import asyncio
import pandas as pd
import numpy as np
from datetime import datetime
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import json

async def main_async(request):
    data = request.get_json()
    store_name = data.get('store_name')
    seller_id = data.get('seller_id')

    print('** Connecting to storage and BigQuery... **')
    # Initialize storage and BigQuery
    storage = CloudStorage(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)

    # Define paths and table names from the config
    bucket_name = settings.BUCKET_STORES
    table_management = settings.TABLE_MANAGEMENT
    destiny_table = settings.TABLE_ORDERS
    blob_shipping_cost = settings.BLOB_ORDERS(store_name)

    # Get dates to treat
    list_dates_to_process = bigquery.get_list_dates_to_process(seller_id, table_management, destiny_table)
    list_dates_to_process = [date.strftime('%Y-%m-%d') for date in list_dates_to_process]

    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process  ***')

    # Use asyncio.gather to process dates asynchronously
    tasks = [process_date(date, storage, bucket_name, blob_shipping_cost) for date in list_dates_to_process]
    results = await asyncio.gather(*tasks)

    # Combine all DataFrames
    df_all_processed_data = pd.concat(results, ignore_index=True)

    print(f'*** Finished processing all dates. Total sales: {df_all_processed_data.shape[0]} ***')

    print('** Deleting existing data **')
    bigquery.delete_existing_data(destiny_table, seller_id, list_dates_to_process, date_filter_name='processed_json')

    print('** Correct dataframe schema **')
    bigquery.match_dataframe_schema(df_all_processed_data, destiny_table)

    print('** Inserting data into BQ **')
    bigquery.insert_dataframe(df_all_processed_data, destiny_table)

    print('** Updating log table **')
    bigquery.update_logs_table(seller_id, list_dates_to_process, destiny_table, table_management)

    return ('Success', 200)

async def process_date(date, storage, bucket_name, blob_shipping_cost):
    try:
        print(f'Processing date: {date}')
        blob_prefix = blob_shipping_cost + f'date={date}/'

        # Use run_in_executor to call synchronous methods without blocking
        loop = asyncio.get_event_loop()
        blobs = await loop.run_in_executor(None, storage.list_blobs, bucket_name, blob_prefix)

        df_processed_data = pd.DataFrame()

        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = await loop.run_in_executor(None, storage.download_json, bucket_name, blob.name)

            for json_content in content:
                df_ = await process_orders(json_content['results'])
                df_processed_data = pd.concat([df_processed_data, df_], ignore_index=True)

        if not df_processed_data.empty:
            df_processed_data['processed_json'] = pd.to_datetime(date)
            df_processed_data['process_time'] = datetime.now()

        print(f'*** Finished treating all data for date {date}. {df_processed_data.shape[0]} sales ***')

        return df_processed_data
    except Exception as e:
        print(f'Error processing date {date}: {e}')
        return pd.DataFrame()

async def process_orders(json_data):
    try:
        df_ = pd.DataFrame()

        for sale in json_data:
            structured_sale = {
                # Your structured_sale dictionary here
            }

            # Create a new DataFrame for the structured sale
            df_sale = pd.DataFrame([structured_sale])

            # Concatenate only if df_ is not empty
            if df_.empty:
                df_ = df_sale  # Assign the first entry directly if df_ is empty
            else:
                df_ = pd.concat([df_, df_sale], ignore_index=True)

        return df_
    except Exception as e:
        print(f'Error processing json: {e}')
        return pd.DataFrame()

def insert_bq_orders(request):
    return asyncio.run(main_async(request))