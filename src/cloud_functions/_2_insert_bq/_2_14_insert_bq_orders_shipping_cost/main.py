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

    # Get dates to treat
    list_dates_to_process = bigquery.get_list_dates_to_process(seller_id, table_management, destiny_table)
    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process ***')

    if not list_dates_to_process:
        print("No dates returned for processing, verify the function get_list_dates_to_process.")
        return ('No dates to process', 200)

    all_processed_data = []  # Lista para acumular todos os DataFrames processados

    for date in list_dates_to_process:
        date_to_process = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        print(f'Processing date: {date}')
        blob_prefix = blob_shipping_cost + f'date={date}/'
        blobs = storage.list_blobs(bucket_name, blob_prefix)

        print(f"Found {len(blobs)} files for prefix {blob_prefix}")
        if len(blobs) == 0:
            print("No files found, check the blob prefix or file structure.")
            continue

        for blob in blobs:
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)

            df_processed_data = transform_to_dataframe_fixed_columns(content)

            if not df_processed_data.empty:
                df_processed_data['correspondent_date'] = pd.to_datetime(date_to_process)
                df_processed_data['process_time'] = datetime.now()
                df_processed_data['seller_id'] = seller_id

                all_processed_data.append(df_processed_data)  # Adiciona o DataFrame à lista

    if not all_processed_data:
        print("No data processed for any date. Skipping insertion.")
        return ('No data to insert', 200)

    # Concatenar todos os DataFrames acumulados
    final_df = pd.concat(all_processed_data, ignore_index=True)
    print(f"Final DataFrame consolidated with {len(final_df)} rows.")

    if not bigquery.table_exists(destiny_table):
        print(f'Table {destiny_table} does not exist. Creating table...')
        bigquery.create_table(destiny_table, final_df)

    print('** Deleting existing data **')
    for date in list_dates_to_process:
        bigquery.delete_existing_data(destiny_table, seller_id, date)

    print('** Correcting dataframe schema **')
    final_df = bigquery.match_dataframe_schema(final_df, destiny_table)

    print('** Inserting data into BQ **')
    bigquery.insert_dataframe(final_df, destiny_table)

    print('** Updating log table **')
    for date in list_dates_to_process:
        bigquery.update_logs_table(seller_id, date, destiny_table, table_management)

    return ('Success', 200)


def transform_to_dataframe_fixed_columns(data_list):
    """
    Transforms a list of JSON dictionaries into a fixed tabular structure.
    Each row represents a combination of shipping_id, sender, and discount.

    Parameters:
        data_list (list): List of JSON dictionaries.

    Returns:
        pd.DataFrame: DataFrame containing the normalized structure.
    """
    transformed_rows = []
    
    for json_item in data_list:
        if json_item:
            # Receiver information (constant for the shipment)
            shipping_id = json_item.get("item_id")
            gross_amount = float(json_item.get("gross_amount", 0))
            receiver = json_item.get("receiver", {})
            
            receiver_data = {
                "shipping_id": shipping_id,
                "gross_amount": gross_amount,
                "receiver_user_id": receiver.get("user_id"),
                "receiver_cost": float(receiver.get("cost", 0)),
                "receiver_save": float(receiver.get("save", 0)),
                "receiver_compensation": float(receiver.get("compensation", 0)),
            }

            # Receiver discounts (separate rows for each discount)
            discounts_receiver = receiver.get("discounts", [])
            for discount in discounts_receiver:
                transformed_rows.append({
                    **receiver_data,
                    "sender_user_id": None,  # No sender for receiver discounts
                    "sender_cost": None,
                    "sender_save": None,
                    "sender_compensation": None,
                    "sender_charges_flex": None,
                    "discount_rate": float(discount.get("rate", 0)),
                    "discount_type": discount.get("type", ""),
                    "discount_promoted_amount": float(discount.get("promoted_amount", 0)),
                })

            # Sender information and their discounts
            senders = json_item.get("senders", [])
            for sender in senders:
                sender_data = {
                    "shipping_id": shipping_id,
                    "gross_amount": gross_amount,
                    "receiver_user_id": receiver.get("user_id"),
                    "receiver_cost": float(receiver.get("cost", 0)),
                    "receiver_save": float(receiver.get("save", 0)),
                    "receiver_compensation": float(receiver.get("compensation", 0)),
                    "sender_user_id": sender.get("user_id"),
                    "sender_cost": float(sender.get("cost", 0)),
                    "sender_save": float(sender.get("save", 0)),
                    "sender_compensation": float(sender.get("compensation", 0)),
                    "sender_charges_flex": float(sender.get("charges", {}).get("charge_flex", 0)),
                }

                # Sender discounts (separate rows for each discount)
                discounts_sender = sender.get("discounts", [])
                for discount in discounts_sender:
                    transformed_rows.append({
                        **sender_data,
                        "discount_rate": float(discount.get("rate", 0)),
                        "discount_type": discount.get("type", ""),
                        "discount_promoted_amount": float(discount.get("promoted_amount", 0)),
                    })

                if not discounts_sender:
                    transformed_rows.append({
                        **sender_data,
                        "discount_rate": None,
                        "discount_type": None,
                        "discount_promoted_amount": None,
                    })

    # Convert the list of dictionaries to a DataFrame
    return pd.DataFrame(transformed_rows)
