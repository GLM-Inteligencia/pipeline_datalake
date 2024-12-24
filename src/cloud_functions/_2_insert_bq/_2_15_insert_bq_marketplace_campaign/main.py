import json
import pandas as pd
from datetime import datetime
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings

def insert_bq_marketplace_campaign(request):
    data = request.get_json()
    store_name = data.get('store_name')
    seller_id = data.get('seller_id')

    print('** Connecting to storage and BigQuery... **')
    storage = CloudStorage(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)

    bucket_name = settings.BUCKET_STORES
    table_management = settings.TABLE_MANAGEMENT
    table_marketplace_campaign = settings.TABLE_MARKETPLACE_CAMPAIGN
    table_item_marketplace_campaign = settings.TABLE_ITEM_MARKETPLACE_CAMPAIGN
    blob_marketplace_campaign = settings.BLOB_MARKETPLACE_CAMPAIGN(store_name)
    blob_item_marketplace_campaign = settings.BLOB_ITEM_MARKETPLACE_CAMPAIGN(store_name)

    today_str = datetime.today().strftime('%Y-%m-%d')
    #list_dates_to_process = [datetime(2024, 12, 24),datetime(2024, 12, 10)]
    
    list_dates_to_process= bigquery.get_list_dates_to_process(seller_id, table_management, table_marketplace_campaign)
    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process ***')

    for date in list_dates_to_process:
        date_to_process = date.strftime('%Y-%m-%d')
        print(f'Processing date: {date_to_process}')

        data_marketplace_campaign = []
        data_item_marketplace_campaign = []

        # Process marketplace_campaign data
        blob_prefix_campaign = blob_marketplace_campaign + f'date={date_to_process}/'
        blobs_campaign = storage.list_blobs(bucket_name, blob_prefix_campaign)
        for blob in blobs_campaign:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)
            if isinstance(content, list):
                for json_item in content:
                    if isinstance(json_item, dict):
                        processed_dict = process_marketplace_campaign(json_item, 'marketplace_campaign')
                        if processed_dict:
                            data_marketplace_campaign.append(processed_dict)
        
        # Process item_marketplace_campaign data
        blob_prefix_item = blob_item_marketplace_campaign + f'date={date_to_process}/'
        blobs_item = storage.list_blobs(bucket_name, blob_prefix_item)
        for blob in blobs_item:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)
            if isinstance(content, list):
                for json_item in content:
                    if isinstance(json_item, dict):
                        processed_list = process_item_marketplace(json_item)
                        if processed_list:
                            for dictionarie in processed_list:
                                data_item_marketplace_campaign.append(dictionarie)

        # Insert data into marketplace_campaign table
        if data_marketplace_campaign:
            df_marketplace = pd.DataFrame(data_marketplace_campaign)
            df_marketplace['correspondent_date'] = pd.to_datetime(date_to_process)
            df_marketplace['process_time'] = datetime.now()
            df_marketplace['seller_id'] = seller_id

            if not bigquery.table_exists(table_marketplace_campaign):
                print(f'Table {table_marketplace_campaign} does not exist. Creating table...')
                bigquery.create_table(table_marketplace_campaign, df_marketplace)

            bigquery.delete_existing_data(table_marketplace_campaign, seller_id, date_to_process)
            df_marketplace = bigquery.match_dataframe_schema(df_marketplace, table_marketplace_campaign)
            bigquery.insert_dataframe(df_marketplace, table_marketplace_campaign)

        #Insert data into item_marketplace_campaign table
        if data_item_marketplace_campaign:
            df_marketplace = pd.DataFrame(data_item_marketplace_campaign)
            df_marketplace['correspondent_date'] = pd.to_datetime(date_to_process)
            df_marketplace['process_time'] = datetime.now()
            df_marketplace['seller_id'] = seller_id

            if not bigquery.table_exists(table_item_marketplace_campaign):
                print(f'Table {table_item_marketplace_campaign} does not exist. Creating table...')
                bigquery.create_table(table_item_marketplace_campaign, df_marketplace)

            bigquery.delete_existing_data(table_item_marketplace_campaign, seller_id, date_to_process)
            df_marketplace = bigquery.match_dataframe_schema(df_marketplace, table_item_marketplace_campaign)
            bigquery.insert_dataframe(df_marketplace, table_item_marketplace_campaign)

        print(f'*** Finished processing date: {date_to_process} ***')

    return ('Success', 200)

def process_marketplace_campaign(json_item, sub_type):
    try:

        # Processar para o sub_type correto
        data = {
            'promotion_id': json_item['id'],
            'status': json_item['status'],
            'type': json_item.get('type'),
            'name': json_item.get('name'),
            'meli_percent': json_item.get('benefits', {}).get('meli_percent'),
            'seller_percent': json_item.get('benefits', {}).get('seller_percent'),
            'benefits_type': json_item.get('benefits', {}).get('type'),
            'start_date': json_item.get('start_date'),
            'finish_date': json_item.get('finish_date'),
            'deadline_date': json_item.get('deadline_date'),
            'sub_type': sub_type,
        }
        return data  # Retorna o dicionário processado

    except Exception as e:
        print(f'Error processing json item: {json_item} | Exception: {e}')
        return None  # Retorna None em caso de erro


def process_item_marketplace(json_data):

    """
    Processa um JSON estruturado e formata os campos disponíveis para inserção no BigQuery.

    Args:
        json_data (list): Dados JSON carregados.

    Returns:
        list: Lista de dicionários formatados para inserção no BigQuery.
    """
    try:

        data = []
        for item in json_data:
            promotion_id = json_data['item_id']

            if item == 'results':
                result =json_data[item]
                if result:
                    for field in result:

                        formatted_data={
                            "id": field.get("id"),
                            "status": field.get("status"),
                            "price": field.get("price"),
                            "original_price": field.get("original_price"),
                            "offer_id": field.get("offer_id"),
                            "meli_percentage": field.get("meli_percentage"),
                            "seller_percentage": field.get("seller_percentage"),
                            "start_date": field.get("start_date"),
                            "end_date": field.get("end_date"),
                            "promotion_id": promotion_id,
                        }

                        data.append(formatted_data)
    except Exception as e:
        print(f'Error processing json item: {json_data} | Exception: {e}')
        return None  # Retorna None em caso de erro
    
    return data