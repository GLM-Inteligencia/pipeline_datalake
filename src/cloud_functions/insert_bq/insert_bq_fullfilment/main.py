import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import json


def insert_bq_fullfilment(request):

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
    destiny_table = settings.TABLE_FULLFILMENT
    blob_shipping_cost = settings.BLOB_FULLFILMENT(store_name)

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
        blob_prefix = blob_shipping_cost + f'date={date_to_process}/'
        # List all the files
        blobs = storage.list_blobs(bucket_name, blob_prefix)

        # Processing each blob
        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)

            for json in content:
                processed_dict = process_fullfilment(json)

                if isinstance(processed_dict, dict):
                    df_processed_data = pd.concat([df_processed_data, pd.DataFrame([processed_dict])], ignore_index = True)
                else:
                    continue

        df_processed_data['correspondent_date'] = pd.to_datetime(date_to_process)
        df_processed_data['process_time'] = datetime.now()
        df_processed_data['seller_id'] = seller_id

        print(f'*** Finished treating all data. {df_processed_data.shape[0]} products ***')

        print('** Deleting existing data **')
        bigquery.delete_existing_data(destiny_table, seller_id, date_to_process)
        
        print(df_processed_data.dtypes)

        print('** Inserting data into BQ**')
        bigquery.insert_dataframe(df_processed_data, destiny_table)

        print('** Updating log table **')
        bigquery.update_logs_table(seller_id, date_to_process, destiny_table, table_management)

    return ('Success', 200)


def process_fullfilment(json):

    try:
        # Extraindo as informações principais
        inventory_id = json['inventory_id']
        total = json['total']
        available_quantity = json['available_quantity']
        not_available_quantity = json['not_available_quantity']

        # Inicializando as colunas 'transfer', 'lost', 'withdrawal' com zero
        status_dict = {'transfer': 0, 'lost': 0, 'withdrawal': 0, 'notSupported': 0}

        # Atualizando os valores baseados no not_available_detail
        for detail in json['not_available_detail']:
            status_dict[detail['status']] = detail['quantity']

        # Retornando os dados como um dicionário
        return {
            'inventory_id': inventory_id,
            'total': total,
            'available_quantity': available_quantity,
            'not_available_quantity': not_available_quantity,
            'transfer': status_dict['transfer'],
            'lost': status_dict['lost'],
            'withdrawal': status_dict['withdrawal'],
            'not_supported':status_dict['notSupported']
        }
    
    except:
        print(f'Error processing json: {json}')
        
                        

