import asyncio
import pandas as pd
import numpy as np
from datetime import datetime
import concurrent.futures
import os
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import json
import pytz


def insert_bq_orders(request):
    return asyncio.run(main_async(request))

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

    # Get dates to process
    loop = asyncio.get_event_loop()
    list_dates_to_process = await loop.run_in_executor(
        None,
        bigquery.get_list_dates_to_process,
        seller_id,
        table_management,
        destiny_table
    )

    list_dates_to_process = [date.strftime('%Y-%m-%d') for date in list_dates_to_process]

    print(f'*** Starting to process dates: {len(list_dates_to_process)} dates to process ***')

    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(20)  # Adjust the value as needed
    if len(list_dates_to_process) != 0:
        # Use asyncio.gather to process dates asynchronously
        tasks = [process_date(date, storage, bucket_name, blob_shipping_cost, semaphore) for date in list_dates_to_process]
        results = await asyncio.gather(*tasks)

        # Combine all DataFrames
        df_all_processed_data = pd.concat(results, ignore_index=True)

        print(f'*** Finished processing all dates. Total sales: {df_all_processed_data.shape[0]} ***')
    
    else:
        print('** 0 dates to process**')
        return ('Success', 200)

    if not bigquery.table_exists(destiny_table):
            print(f'Table {destiny_table} does not exist. Creating table...')
            bigquery.create_table(destiny_table, df_all_processed_data)

    # The following steps are synchronous and don't need to be async
    print('** Deleting existing data **')
    bigquery.delete_existing_data(destiny_table, seller_id, list_dates_to_process, 'processed_json')

    print('** Correcting dataframe schema **')
    bigquery.match_dataframe_schema(df_all_processed_data, destiny_table)

    print('** Inserting data into BigQuery **')
    bigquery.insert_dataframe(df_all_processed_data, destiny_table)

    print('** Updating log table **')
    bigquery.update_logs_table(seller_id, list_dates_to_process, destiny_table, table_management)

    return ('Success', 200)

async def process_date(date, storage, bucket_name, blob_shipping_cost, semaphore):
    async with semaphore:
        try:
            print(f'Processing date: {date}')
            blob_prefix = blob_shipping_cost + f'date={date}/'

            loop = asyncio.get_event_loop()
            blobs = await loop.run_in_executor(None, storage.list_blobs, bucket_name, blob_prefix)

            # Create a semaphore for blob processing
            blob_semaphore = asyncio.Semaphore(25)  # Adjust as needed

            tasks = [process_blob(blob, storage, bucket_name, blob_semaphore) for blob in blobs]
            results = await asyncio.gather(*tasks)
            df_processed_data = pd.concat(results, ignore_index=True)

            if not df_processed_data.empty:
                df_processed_data['processed_json'] = pd.to_datetime(df_processed_data['date_created'])
                date_now_str = datetime.now()
                date_now_str_formatted = change_time_zone(date_now_str)
                df_processed_data['process_time'] = date_now_str_formatted

            print(f'*** Finished processing all data for date {date}. {df_processed_data.shape[0]} sales ***')

            return df_processed_data
        except Exception as e:
            print(f'Error processing date {date}: {e}')
            return pd.DataFrame()

async def process_blob(blob, storage, bucket_name, semaphore):
    async with semaphore:
        try:
            print(f"Reading file: {blob.name}")
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, storage.download_json, bucket_name, blob.name)

            # Use ThreadPoolExecutor for compatibility in async context
            with concurrent.futures.ThreadPoolExecutor() as executor:
                df_list = list(executor.map(process_orders_sync, [json_content['results'] for json_content in content]))

            df_blob = pd.concat(df_list, ignore_index=True)
            return df_blob
        except Exception as e:
            print(f'Error processing blob {blob.name}: {e}')
            return pd.DataFrame()

def change_time_zone(date_str):

    date_approved = datetime.fromisoformat(date_str)
    saopaulo_tz = pytz.timezone('America/Sao_Paulo')
    date_saopaulo = date_approved.astimezone(saopaulo_tz)
    date_formatted = date_saopaulo.isoformat()

    return date_formatted

def process_orders_sync(json_data):
    try:
        structured_sales = []  # List to collect all structured_sale dictionaries
        for sale in json_data:
            date_approved_str = sale['payments'][0].get('date_approved')
            date_approved_formatted = change_time_zone(date_approved_str)

            date_last_modified_str = sale['payments'][0].get('date_last_modified')
            date_last_modified_formatted = change_time_zone(date_last_modified_str)

            date_created_str = sale.get('date_created')
            date_created_formatted = change_time_zone(date_created_str)

            date_closed_str = sale.get('date_closed')
            date_closed_formatted = change_time_zone(date_closed_str)

            date_last_updated_str = sale.get('date_last_updated')
            date_last_updated_formatted = change_time_zone(date_last_updated_str)

            structured_sale = {
                'reason': sale['payments'][0].get('reason'),
                'status_code': sale['payments'][0].get('status_code'),
                'total_paid_amount': sale['payments'][0].get('total_paid_amount'),
                'operation_type': sale['payments'][0].get('operation_type'),
                'transaction_amount': sale['payments'][0].get('transaction_amount'),
                'transaction_amount_refunded': sale['payments'][0].get('transaction_amount_refunded'),
                'date_approved': date_approved_formatted,
                'collector_id': sale['payments'][0].get('collector', {}).get('id'),
                'coupon_id': sale['payments'][0].get('coupon_id'),
                'installments': sale['payments'][0].get('installments'),
                'authorization_code': sale['payments'][0].get('authorization_code'),
                'taxes_amount': sale['payments'][0].get('taxes_amount'),
                'payment_id': sale['payments'][0].get('id'),
                'date_last_modified': date_last_modified_formatted,
                'coupon_amount': sale['payments'][0].get('coupon_amount'),
                'installment_amount': sale['payments'][0].get('installment_amount'),
                'activation_uri': sale['payments'][0].get('activation_uri'),
                'overpaid_amount': sale['payments'][0].get('overpaid_amount'),
                'card_id': sale['payments'][0].get('card_id'),
                'issuer_id': sale['payments'][0].get('issuer_id'),
                'payment_method_id': sale['payments'][0].get('payment_method_id'),
                'payment_type': sale['payments'][0].get('payment_type'),
                'deferred_period': sale['payments'][0].get('deferred_period'),
                'atm_transfer_reference_transaction_id': sale['payments'][0].get('atm_transfer_reference', {}).get('transaction_id'),
                'atm_transfer_reference_company_id': sale['payments'][0].get('atm_transfer_reference', {}).get('company_id'),
                'site_id': sale['payments'][0].get('site_id'),
                'payer_id': sale['payments'][0].get('payer_id'),
                'order_id': sale['payments'][0].get('order_id'),
                'currency_id': sale['payments'][0].get('currency_id'),
                'payment_status': sale['payments'][0].get('status'),
                'shipping_id': sale.get('shipping', {}).get('id'),
                'fulfilled': sale.get('fulfilled'),
                'seller_id': sale.get('seller', {}).get('id'),
                'buyer_id': sale.get('buyer', {}).get('id'),
                'item_id': sale['order_items'][0]['item'].get('id'),
                'item_title': sale['order_items'][0]['item'].get('title'),
                'item_category_id': sale['order_items'][0]['item'].get('category_id'),
                'item_variation_id': sale['order_items'][0]['item'].get('variation_id'),
                'seller_custom_field': sale['order_items'][0]['item'].get('seller_custom_field'),
                'global_price': sale['order_items'][0]['item'].get('global_price'),
                'net_weight': sale['order_items'][0]['item'].get('net_weight'),
                'warranty': sale['order_items'][0]['item'].get('warranty'),
                'condition': sale['order_items'][0]['item'].get('condition'),
                'seller_sku': sale['order_items'][0]['item'].get('seller_sku'),
                'quantity': sale['order_items'][0].get('quantity'),
                'unit_price': sale['order_items'][0].get('unit_price'),
                'full_unit_price': sale['order_items'][0].get('full_unit_price'),
                'manufacturing_days': sale['order_items'][0].get('manufacturing_days'),
                'requested_quantity_measure': sale['order_items'][0].get('requested_quantity', {}).get('measure'),
                'requested_quantity_value': sale['order_items'][0].get('requested_quantity', {}).get('value'),
                'sale_fee': sale['order_items'][0].get('sale_fee'),
                'listing_type_id': sale['order_items'][0].get('listing_type_id'),
                'base_exchange_rate': sale['order_items'][0].get('base_exchange_rate'),
                'base_currency_id': sale['order_items'][0].get('base_currency_id'),
                'bundle': sale['order_items'][0].get('bundle'),
                'element_id': sale['order_items'][0].get('element_id'),
                'date_created': date_created_formatted,
                'date_closed': date_closed_formatted,
                'status': sale.get('status'),
                'expiration_date': sale.get('expiration_date'),
                'date_last_updated': date_last_updated_formatted,
                'last_updated': sale.get('last_updated'),
                'comment': sale.get('comment'),
                'pack_id': sale.get('pack_id'),
                'coupon_amount': sale.get('coupon', {}).get('amount'),
                'coupon_id': sale.get('coupon', {}).get('id'),
                'shipping_cost': sale.get('shipping_cost'),
                'pickup_id': sale.get('pickup_id'),
                'status_detail': sale.get('status_detail'),
                'total_amount': sale.get('total_amount'),
                'paid_amount': sale.get('paid_amount'),
                'context_application': sale.get('context', {}).get('application'),
                'context_product_id': sale.get('context', {}).get('product_id'),
                'context_channel': sale.get('context', {}).get('channel'),
                'context_site': sale.get('context', {}).get('site'),
            }
            structured_sales.append(structured_sale)  # Collect dictionaries
        df_ = pd.DataFrame(structured_sales)  # Create DataFrame once
        return df_
    except Exception as e:
        print(f'Error processing JSON data: {e}')
        return pd.DataFrame()
