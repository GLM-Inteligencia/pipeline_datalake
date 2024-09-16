import pandas as pd
import numpy as np

from datetime import datetime
from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import json 


def insert_bq_orders(request):

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
                df_ = process_orders(json)
                
                df_processed_data = pd.concat([df_processed_data, df_], ignore_index = True)

        df_processed_data['correspondent_date'] = pd.to_datetime(date_to_process)
        df_processed_data['process_time'] = datetime.now()

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

def process_orders(json):

    try:
        df_ = pd.DataFrame()

        for sale in json['results']:

            structured_sale = {
              'reason': sale['payments'][0].get('reason'),
              'status_code': sale['payments'][0].get('status_code'),
              'total_paid_amount': sale['payments'][0].get('total_paid_amount'),
              'operation_type': sale['payments'][0].get('operation_type'),
              'transaction_amount': sale['payments'][0].get('transaction_amount'),
              'transaction_amount_refunded': sale['payments'][0].get('transaction_amount_refunded'),
              'date_approved': sale['payments'][0].get('date_approved'),
              'collector_id': sale['payments'][0]['collector'].get('id') if 'collector' in sale['payments'][0] else None,
              'coupon_id': sale['payments'][0].get('coupon_id'),
              'installments': sale['payments'][0].get('installments'),
              'authorization_code': sale['payments'][0].get('authorization_code'),
              'taxes_amount': sale['payments'][0].get('taxes_amount'),
              'payment_id': sale['payments'][0].get('id'),
              'date_last_modified': sale['payments'][0].get('date_last_modified'),
              'coupon_amount': sale['payments'][0].get('coupon_amount'),
              'shipping_cost': sale['payments'][0].get('shipping_cost'),
              'installment_amount': sale['payments'][0].get('installment_amount'),
              'activation_uri': sale['payments'][0].get('activation_uri'),
              'overpaid_amount': sale['payments'][0].get('overpaid_amount'),
              'card_id': sale['payments'][0].get('card_id'),
              'status_detail': sale['payments'][0].get('status_detail'),
              'issuer_id': sale['payments'][0].get('issuer_id'),
              'payment_method_id': sale['payments'][0].get('payment_method_id'),
              'payment_type': sale['payments'][0].get('payment_type'),
              'deferred_period': sale['payments'][0].get('deferred_period'),
              'atm_transfer_reference_transaction_id': sale['payments'][0]['atm_transfer_reference'].get('transaction_id') if 'atm_transfer_reference' in sale['payments'][0] else None,
              'atm_transfer_reference_company_id': sale['payments'][0]['atm_transfer_reference'].get('company_id') if 'atm_transfer_reference' in sale['payments'][0] else None,
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
              'requested_quantity_measure': sale['order_items'][0]['requested_quantity'].get('measure') if 'requested_quantity' in sale['order_items'][0] else None,
              'requested_quantity_value': sale['order_items'][0]['requested_quantity'].get('value') if 'requested_quantity' in sale['order_items'][0] else None,
              'sale_fee': sale['order_items'][0].get('sale_fee'),
              'listing_type_id': sale['order_items'][0].get('listing_type_id'),
              'base_exchange_rate': sale['order_items'][0].get('base_exchange_rate'),
              'base_currency_id': sale['order_items'][0].get('base_currency_id'),
              'bundle': sale['order_items'][0].get('bundle'),
              'element_id': sale['order_items'][0].get('element_id'),
              'date_created': sale.get('date_created'),
              'date_closed': sale.get('date_closed'),
              'status': sale.get('status'),
              'expiration_date': sale.get('expiration_date'),
              'date_last_updated': sale.get('date_last_updated'),
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

            df_ = pd.concat([df_, pd.DataFrame([structured_sale])], ignore_index = True)
    
    except Exception as e:
        print(f'Error processing json: {e}')
    
    return df_

        
                        

