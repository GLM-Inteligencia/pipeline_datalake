import pandas as pd
from datetime import datetime
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings

def insert_bq_promotions(request):
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
    destiny_table = settings.TABLE_ITEM_PROMOTION
    blob_promotions = settings.BLOB_PROMOTIONS(store_name)
    blob_promotions_mshops = settings.BLOB_PROMOTIONS_MSHOPS(store_name)

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Get dates to treat
    list_dates_to_process = bigquery.get_list_dates_to_process(seller_id, table_management, destiny_table)

    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process ***')

    for date in list_dates_to_process:
        date_to_process = date.strftime('%Y-%m-%d')
        print(f'Processing date: {date_to_process}')

        processed_data = []

        # Channel 'marketplace' processing
        blob_prefix = blob_promotions + f'date={date_to_process}/'
        blobs = storage.list_blobs(bucket_name, blob_prefix)
        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)
            for json_item in content:
                if json_item:  # Ensure json_item is not None or empty
                    for item in json_item:
                        if item:
                            processed_dict = process_response(item, 'Marketplace')
                            if processed_dict:
                                processed_data.append(processed_dict)

        # Channel 'mshops' processing
        blob_prefix_mshops = blob_promotions_mshops + f'date={date_to_process}/'
        blobs = storage.list_blobs(bucket_name, blob_prefix_mshops)
        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)
            for json_item in content:
                if json_item:  # Ensure json_item is not None or empty
                    for item in json_item:
                        if item:
                            processed_dict = process_response(item, 'mshops')
                            if processed_dict:
                                processed_data.append(processed_dict)

        df_processed_data = pd.DataFrame(processed_data)

        if df_processed_data.empty:
            print(f'Nenhum dado processado para a data {date_to_process}, pulando inserção...')
            continue

        df_processed_data['correspondent_date'] = pd.to_datetime(date_to_process)
        df_processed_data['process_time'] = datetime.now()
        df_processed_data['seller_id'] = seller_id

        print(f'*** Finished treating all data. {df_processed_data.shape[0]} products ***')

        if not bigquery.table_exists(destiny_table):
            print(f'Table {destiny_table} does not exist. Creating table...')
            bigquery.create_table(destiny_table, df_processed_data)

        print('** Deleting existing data **')
        bigquery.delete_existing_data(destiny_table, seller_id, date_to_process)

        print('** Correcting dataframe schema **')
        df_processed_data = bigquery.match_dataframe_schema(df_processed_data, destiny_table)

        print('** Inserting data into BQ **')
        bigquery.insert_dataframe(df_processed_data, destiny_table)

        print('** Updating log table **')
        bigquery.update_logs_table(seller_id, date_to_process, destiny_table, table_management)

    return ('Success', 200)


def process_response(json_item, channel):

    try:
        # Define required fields for both channels
        required_fields_marketplace = ['item_id', 'id', 'status', 'type', 'name', 'start_date', 'finish_date']
        required_fields_mshops = ['item_id', 'id', 'status', 'type', 'name', 'target', 'buy_quantity', 'start_date', 'finish_date']

        required_fields = required_fields_marketplace if channel == "Marketplace" else required_fields_mshops

        # Check if all required fields are present in the json_item
        for field in required_fields:
            if field not in json_item or json_item[field] is None:
                # Check within "offers" list for fields like "start_date"
                if field == "start_date" and "offers" in json_item:
                    start_date_found = any(offer.get("start_date") for offer in json_item["offers"])
                    if not start_date_found:
                        print(f'Missing or None field "{field}" in item: {json_item}')

        # Process data based on channel type
        if channel == "Marketplace":
            data = {
                'item_id': json_item['item_id'],
                'promotion_id': json_item['id'],
                'status': json_item['status'],
                'type': json_item['type'],
                'name': json_item['name'],
                'meli_percent': json_item.get('benefits', {}).get('meli_percent'),
                'seller_percent': json_item.get('benefits', {}).get('seller_percent'),
                'start_date': json_item['start_date'] if 'start_date' in json_item else json_item['offers'][0].get('start_date'),
                'finish_date': json_item['finish_date'],
                'channel': channel,
            }

        elif channel == "mshops":
            data = {
                'item_id': json_item['item_id'],
                'promotion_id': json_item['id'],
                'status': json_item['status'],
                'type': json_item['type'],
                'name': json_item['name'],
                'target': json_item['target'],
                'buy_quantity': json_item['buy_quantity'],
                'start_date': json_item['start_date'] if 'start_date' in json_item else json_item['offers'][0].get('start_date'),
                'finish_date': json_item['finish_date'],
                'channel': channel,
            }

        return data  # Return the processed data dictionary
    
    except Exception as e:
            print(f'Error processing json item: {json_item} | Exception: {e}')
            return None  # Return None in case of an exception to avoid errors in the main flow