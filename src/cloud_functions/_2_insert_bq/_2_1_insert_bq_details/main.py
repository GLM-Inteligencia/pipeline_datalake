import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import json


def insert_bq_details(request):

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
    destiny_table = settings.TABLE_DETAILS
    blob_details = settings.BLOB_ITEMS_DETAILS(store_name)
    blob_variations = settings.BLOB_VARIATIONS(store_name)

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Get dates to treat
    list_dates_to_process = bigquery.get_list_dates_to_process(seller_id, table_management, destiny_table)
    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process  ***')
    df_processed_data = pd.DataFrame()
    for date in list_dates_to_process:
        # Transform date to string
        date_to_process = date.strftime('%Y-%m-%d')
        print(f'Processing date: {date_to_process}')
        # Get blob with the date
        blob_prefix_details = blob_details + f'date={date_to_process}/'
        blob_prefix_variations = blob_variations + f'date={date_to_process}/'
        # List all the files
        blobs_details = storage.list_blobs(bucket_name, blob_prefix_details)
        blobs_variations = storage.list_blobs(bucket_name, blob_prefix_variations)
        
        # Empty variables
        df_processed_data = pd.DataFrame()
        content_details=[]
        content_variations=[]

        # Getting details data
        for blob_det in blobs_details:
            # Get content information for details and variations
            print(f"Reading file: {blob_det.name}")
            content_details += storage.download_json(bucket_name, blob_det.name)

        # Getting variation data
        for blob_var in blobs_variations:
            print(f"Reading file: {blob_var.name}")
            content_variations += storage.download_json(bucket_name, blob_var.name)

        df_processed_data = process_details(content_details, content_variations)

        df_processed_data['correspondent_date'] = pd.to_datetime(date_to_process)
        df_processed_data['process_time'] = datetime.now()
        df_processed_data['seller_id'] = seller_id

        print(f'*** Finished treating all data. {df_processed_data.shape[0]} products ***')

        print('** Deleting existing data **')
        bigquery.delete_existing_data(destiny_table, seller_id, date_to_process)
        
        print('** Correct dataframe schema **')
        bigquery.match_dataframe_schema(df_processed_data, destiny_table)

        print('** Inserting data into BQ**')
        bigquery.insert_dataframe(df_processed_data, destiny_table)

        print('** Updating log table **')
        bigquery.update_logs_table(seller_id, date_to_process, destiny_table, table_management)

    return ('Success', 200)


def extract_seller_sku(attributes):
    for attribute in attributes:
        if attribute.get('id') == 'SELLER_SKU':
            return attribute.get('value_name')
    return None  

def process_details(content_details, content_variations):  

    df_product = pd.DataFrame()

        # Checking if item has variations
    for item in content_details:

        if extract_seller_sku(item.get('attributes', [])):
            has_variation = False
        else:
            has_variation = True   
        # get general information
        product_details_general = {
            'item_id': item.get('id'),
            'item_name': item.get('title'),
            'seller_id': item.get('seller_id'),
            'category_id': item.get('category_id'),
            'official_store_id': item.get('official_store_id'),
            'price': item.get('price'),
            'base_price': item.get('base_price'),
            'original_price': item.get('original_price'),
            'initial_quantity': item.get('initial_quantity'),
            'status': item.get('status'),
            'listing_type': item.get('listing_type_id'),
            'url': item.get('permalink'),
            'free_shipping': item.get('shipping',{}).get('free_shipping'),
            'catalog_id' : item.get('catalog_product_id'),
            'picture_url': item.get('pictures', [{}])[0].get('url'),
            'catalog_listing': item.get('catalog_listing', ''),
            'item_health': item.get('health',''),
        }  
        # If product does not have variations
        if not has_variation:
            product_detail_variation = {
                'inventory_id': item.get('inventory_id'),
                'currency_id': item.get('currency_id'),
                'stock': item.get('available_quantity'),
                'sold_quantity': item.get('sold_quantity'),
                'seller_sku': extract_seller_sku(item.get('attributes', [])),
                'variation_id': np.nan
            }
            product_details_general.update(product_detail_variation)
            df_ = pd.DataFrame([product_details_general])
            df_product = pd.concat([df_product, df_], ignore_index=True)

        # If product has variations
        else:
            for var in item.get('variations', []):
                variation_id = var['id']
                variation = [variation for variation in content_variations if variation['id'] == variation_id][0]
                variation_id = var['id']
                product_detail_variation = {
                    'inventory_id': variation.get('inventory_id'),
                    'currency_id': variation.get('currency_id'),
                    'stock': variation.get('available_quantity'),
                    'sold_quantity': variation.get('sold_quantity'),
                    'seller_sku': extract_seller_sku(variation.get('attributes', [])),
                    'variation_id': variation_id
                }
                product_details_general.update(product_detail_variation)
                df_ = pd.DataFrame([product_details_general])
                df_product = pd.concat([df_product, df_], ignore_index=True)

    return df_product
