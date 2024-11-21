import pandas as pd
from datetime import datetime, timedelta
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings

def insert_bq_orders_shipping_cost(request):

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
    destiny_table = settings.TABLE_ORDERS_SHIPPING_COST
    blob_shipping_cost = settings.BLOB_ORDERS_SHIPPING_COST(store_name)

    # Define today's date
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Get dates to treat
    list_dates_to_process = bigquery.get_list_dates_to_process(seller_id, table_management, destiny_table)

    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process ***')

    for date in list_dates_to_process:
        date_to_process = date.strftime('%Y-%m-%d')
        print(f'Processing date: {date_to_process}')

        blob_prefix = blob_shipping_cost + f'date={date_to_process}/'
        blobs = storage.list_blobs(bucket_name, blob_prefix)
        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)

            df_processed_data = transform_to_dataframe(content)

        if df_processed_data.empty:
            print(f'No data processed for the date {date_to_process}, skipping insertion...')
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


def transform_to_dataframe(data_list):
    transformed_data = []
    
    for json_item in data_list:
        receiver = json_item.get("receiver", {})
        discounts_receiver = receiver.get("discounts", [])
        cost_details = receiver.get("cost_details", [])
        senders = json_item.get("senders", [])
        
        # Data base for item and receiver
        data = {
            "shipping_id": json_item.get("item_id", None),
            "gross_amount": json_item.get("gross_amount", 0),
            "receiver_user_id": receiver.get("user_id", None),
            "receiver_cost": receiver.get("cost", 0),
            "receiver_save": receiver.get("save", 0),
            "receiver_compensation": receiver.get("compensation", 0),
            "receiver_discounts": str(discounts_receiver),  # Serialize lists as strings
            "receiver_cost_details": str(cost_details),  # Serialize lists as strings
            "receiver_discounts_count": len(discounts_receiver),  # Number of receiver discounts
        }

        # Receiver discounts details
        for i, discount in enumerate(discounts_receiver):
            data[f"receiver_discount_{i+1}_rate"] = discount.get("rate", 0)
            data[f"receiver_discount_{i+1}_type"] = discount.get("type", "")
            data[f"receiver_discount_{i+1}_promoted_amount"] = discount.get("promoted_amount", 0)
        
        # Senders details
        for j, sender in enumerate(senders):
            sender_key = f"sender_{j+1}_"
            sender_discounts = sender.get("discounts", [])
            
            # Add sender's basic information
            data.update({
                f"{sender_key}user_id": sender.get("user_id", None),
                f"{sender_key}cost": sender.get("cost", 0),
                f"{sender_key}save": sender.get("save", 0),
                f"{sender_key}compensation": sender.get("compensation", 0),
                f"{sender_key}charges_flex": sender.get("charges", {}).get("charge_flex", 0),
                f"{sender_key}discounts_count": len(sender_discounts),  # Number of sender discounts
                f"{sender_key}discounts": str(sender_discounts)  # Serialize discounts
            })

            # Sender discounts details
            for k, discount in enumerate(sender_discounts):
                data.update({
                    f"{sender_key}discount_{k+1}_rate": discount.get("rate", 0),
                    f"{sender_key}discount_{k+1}_type": discount.get("type", ""),
                    f"{sender_key}discount_{k+1}_promoted_amount": discount.get("promoted_amount", 0)
                })
        
        # Add the transformed data to the list
        transformed_data.append(data)
    
    # Create the DataFrame
    return pd.DataFrame(transformed_data)



