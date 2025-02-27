from src.common.cloud_storage_connector import CloudStorage
from src.common.bigquery_connector import BigQueryManager
from src.common.trigger_cloud_function import TriggerCloudFunction

from src.common.utils import authenticate, fetch_sales_for_day, log_process
from src.config import settings
from datetime import datetime, timedelta
from flask import jsonify


def fetch_orders_data(request):
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

    # Define today's date
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    print(f'** Cleaning blob **')
    # Path for saving 
    blob_basic_path = settings.BLOB_ORDERS(store_name)
    date_blob_path = f'{blob_basic_path}date={yesterday_str}/'
    destiny_table = settings.TABLE_ORDERS

    # Check if it is the first time treating orders for the client
    bool_first_time = storage.blob_exists(bucket_name, blob_basic_path)

    if not bool_first_time:
        print('** First time processing orders - processing history **')
        trigger_functions = TriggerCloudFunction(credentials_path=settings.PATH_SERVICE_ACCOUNT)
            
        trigger_functions.trigger_function(function_url='https://southamerica-east1-datalake-v2-424516.cloudfunctions.net/fetch_history_orders',
                                           params= data) 
                             
        return ('Success', 200)
    
    else:
        # Clean existing files in the storage bucket
        storage.clean_blobs(bucket_name, date_blob_path)

        print(f'** Starting API requests **')
        # URL function for API
        url = settings.URL_ORDERS
        headers = {'Authorization': f'Bearer {access_token}'}

        # Fetch and save all items
        num_sales, all_responses = fetch_sales_for_day(url(seller_id), access_token)

        # creating file path
        timezone_offset = "-03:00" # Brazilian time
        process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f{timezone_offset}")
        file_name = f'total_sales={num_sales}__data={yesterday_str}__processing-time={process_time}.json'
        file_path = date_blob_path + file_name 

        # saving files
        storage.upload_json(bucket_name, file_path, all_responses)

        # Log the process in BigQuery
        log_process(seller_id, destiny_table, yesterday_str, table_management, processed_to_bq=False)
    
    return jsonify({"message": "Success", "status_code":200})
