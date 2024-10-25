import asyncio
import json
from datetime import datetime, timedelta
import requests 
import time



async def batch_process(session, items, url_func_or_string, headers, 
                        bucket_name, date_blob_path, storage, url_with_two_fields=False,
                        chunk_size=100, add_item_id=False, params=None, sleep_time = 1):
    """
    Processes API requests in batches and stores the responses in Google Cloud Storage.

    Args:
        session: The aiohttp session.
        items (list): List of items to process.
        url_func_or_string (str or callable): A URL string or function to generate the URL.
        headers (dict): Headers for the API requests.
        bucket_name (str): Name of the storage bucket.
        date_blob_path (str): Path for saving the results.
        storage (CloudStorage): Storage instance.
        chunk_size (int, optional): Size of the chunks for batch processing. Defaults to 100.
        add_item_id (bool, optional): Whether to include the item_id in the response data. Defaults to False.
        params (list, optional): List of additional parameters to pass in the request for each item.

    Returns:
        None
    """
    semaphore = asyncio.Semaphore(100)
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    
    # --- Start of changes ---
    if params:
        params_chunks = [params[i:i + chunk_size] for i in range(0, len(params), chunk_size)]
    else:
        params_chunks = [None] * len(chunks)  # Ensure params_chunks aligns with chunks

    async def process_chunk(chunk, batch_number, params_chunk):
        if params_chunk:
            # Use params_chunk (singular) to represent the parameters for this chunk
            tasks = [
                fetch(
                    session, item, url_func_or_string, headers, semaphore, add_item_id, 
                    param, url_with_two_fields
                ) for item, param in zip(chunk, params_chunk)
            ]
        else:
            tasks = [
                fetch(
                    session, item, url_func_or_string, headers, semaphore, add_item_id, 
                    params=None, url_with_two_fields=url_with_two_fields
                ) for item in chunk
            ]

        # Use return_exceptions=True to maintain order even if exceptions occur
        responses = await asyncio.gather(*tasks, return_exceptions=True)

        if responses:
            filtered_responses = []
            for response in responses:
                if isinstance(response, Exception):
                    # Convert the exception to a string
                    filtered_responses.append(str(response))
                else:
                    # Add the item normally if it is serializable
                    filtered_responses.append(response)

            # Save batch responses to Cloud Storage
            process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f-03:00")
            filename = f'batch_{batch_number}__process_time={process_time}.json'
            storage.upload_json(bucket_name, f'{date_blob_path}{filename}', filtered_responses)

    # Correctly pass params_chunk to process_chunk
    for batch_number, (chunk, params_chunk) in enumerate(zip(chunks, params_chunks)):
        await process_chunk(chunk, batch_number, params_chunk)
        await asyncio.sleep(sleep_time)  # Sleep to avoid rate limits


async def fetch(session, item_id, url_func_or_string, headers, 
                semaphore, add_item_id, params=None, url_with_two_fields=False):
    """
    Fetches data for a single item using either a URL function or a URL string.

    Args:
        session: The aiohttp session.
        item_id (str): The item ID to be passed.
        url_func_or_string (str or callable): A URL string or function to generate the URL.
        headers (dict): Headers for the API requests.
        semaphore (asyncio.Semaphore): Semaphore to control the number of concurrent requests.
        add_item_id (bool): Whether to include the item_id in the response data.
        params (dict, optional): Additional parameters to pass in the request.
        seller_id (str, optional): Seller ID to be passed to the URL function.

    Returns:
        dict: The JSON response data.
    """
    async with semaphore:
        # Determine the URL (either a function or a fixed string)
        if callable(url_func_or_string):
            if url_with_two_fields:
                url=url_func_or_string(item_id['item_id'], item_id['variation_id'])
            else:
                url = url_func_or_string(item_id)  # Call the URL function with just item_id
        else:
            url = url_func_or_string  # Fixed URL string
        
        # Make the request with params (if provided)
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()  # Await the json() method

                if add_item_id and isinstance(data, dict):
                    data['item_id'] = item_id

                elif add_item_id and isinstance(data, list):
                    data.append({"item_id" : item_id})

                return data
            elif response.status == 404:
                print(f"Item ID {item_id} not found (404). Skipping...")
            else:
                # Handle other errors (e.g., 400, 500, etc.)
                error_message = await response.text()  # Get the error message from the response
                print(f"Error for item ID {item_id}: Status code {response.status}, Response: {error_message}")


def log_process(seller_id, table_name, process_date, management_table, processed_to_bq=False):
    from google.cloud import bigquery
    client = bigquery.Client()
    
    # Update management log in BigQuery
    query = f"""
    DELETE FROM {management_table}
    WHERE seller_id = {seller_id} 
      AND table_name = '{table_name}'
      AND date(process_date) = '{process_date}';
    """
    client.query(query).result()

    insert_query = f"""
    INSERT INTO {management_table}
    (seller_id, table_name, process_date, processed_to_bq, last_bq_processing) 
    VALUES ({seller_id}, '{table_name}', '{process_date}', {processed_to_bq}, CURRENT_TIMESTAMP());
    """
    client.query(insert_query).result()

def authenticate(client_id, client_secret):
  url = "https://api.mercadolibre.com/oauth/token"
  payload = {
      'grant_type': 'client_credentials',
      'client_id': client_id,
      'client_secret': client_secret
  }
  response = requests.post(url, data=payload)
  if response.status_code == 200:
    return response.json()['access_token']
  else:
    raise Exception("Authentication failed")

def extract_seller_sku(attributes):
    for attribute in attributes:
        if attribute.get('id') == 'SELLER_SKU':
            return attribute.get('value_name')
    return None  

def fetch_items_from_storage(storage, bucket_name, blob_items_prefix, key_names, filter_key=None, filter_value=None):
    """
    Fetches items from Google Cloud Storage, with optional filtering based on a specific key-value pair.
    Supports fetching either a single key (string) or multiple keys (list) from the JSON files.

    Args:
        storage (CloudStorage): CloudStorage instance to interact with GCS.
        bucket_name (str): The name of the bucket to fetch from.
        blob_items_prefix (str): The prefix path to the blobs to read.
        key_names (str or list): A single key or a list of keys to extract from each item in the JSON files.
        filter_key (str, optional): The key to filter items by (if provided). Defaults to None.
        filter_value (any, optional): The value to filter items by (if provided). Defaults to None.

    Returns:
        list: A list of unique, non-null items (either single values or dictionaries).
    """
    # Ensure key_names is a list if it's a string
    is_single_key = isinstance(key_names, str)
    if is_single_key:
        key_names = [key_names]

    items_list = []
    blobs = storage.list_blobs(bucket_name, blob_items_prefix)

    for blob in blobs:
        if blob.name.endswith('.json'):
            print(f"Reading file: {blob.name}")
            content = storage.download_json(bucket_name, blob.name)

            for item in content:
                # If filtering by key-value pair, only include matching items
                if filter_key is None or item.get(filter_key) == filter_value:
                    if is_single_key:
                        # Single key: Append the value of the key directly
                        value = item.get(key_names[0], None)
                        if isinstance(value, list):
                            items_list.extend(value)
                        elif value is not None:  # Filter out None values
                            items_list.append(value)
                    else:
                        # Multiple keys: Create a dictionary of key-value pairs
                        item_dict = {key: item.get(key, None) for key in key_names}
                        if all(value is not None for value in item_dict.values()):  # Ensure all values are not None
                            items_list.append(item_dict)

    # Return a list of unique values if it's a single key, otherwise list of dictionaries
    return list(set(items_list)) if is_single_key else [dict(t) for t in {tuple(d.items()) for d in items_list}]


def fetch_items(url, seller_id, access_token):
    url = url(seller_id)
    params = {
        'access_token': access_token,
        'limit': 50,  # You can set this up to 100
        'search_type': 'scan'
    }
    
    # First request
    response = requests.get(url, params=params)
    data = response.json()

    # Get the initial scroll_id from the response
    scroll_id = data.get('scroll_id', None)
    all_items = data.get('results', [])
    all_responses = [data]
    while scroll_id:
        # Update the request to use the scroll_id
        params.update({'scroll_id': scroll_id})
        
        # Request the next page using scroll_id
        response = requests.get(url, params=params)
        data = response.json()
        all_responses.append(data)

        # Add the new items to the list
        all_items.extend(data.get('results', []))
        
        # Get the next scroll_id (if any)
        scroll_id = data.get('scroll_id', None)
        
        if not data.get('results'):  # Stop when no more results
            break

    print(f'** Items found: {len(all_items)} **')
    return all_items, all_responses

def fetch_sales_for_day(url, access_token, timezone_offset='-03:00', limit=50):
    # Set default dates (yesterday by default)
    start_request_date = datetime.today() - timedelta(days=1)

    # Set API params for the current day
    start_date = start_request_date.strftime(f"%Y-%m-%dT00:00:00.000{timezone_offset}")
    end_date = start_request_date.strftime(f"%Y-%m-%dT23:59:59.999{timezone_offset}") 
    params = {
        'access_token': access_token,
        'limit': limit,
        'order.date_created.from': start_date,
        'order.date_created.to': end_date
    }   

    offset = 0
    count_total_results = 0
    all_responses = []  
    all_sales = [] 

    print(f'** Starting request for day {start_request_date} **') 
    print(f'Start date {start_date}')
    print(f'End date {end_date}')

    while True:
        # Update the offset
        params['offset'] = offset       
        # Request the API
        response = requests.get(url ,params=params)     
        # Check the request status
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.json()}")     
        # Get sales data
        sales_data = response.json()
        results = sales_data['results']
        print(f"Offset {offset}: {len(results)} sales")     
        if sales_data['paging']['total'] <= 0:
            print(f'** Request for day {start_request_date} is empty. **')
            break       
        # Add the JSON response to the list of all responses
        all_responses.append(sales_data)
        all_sales.append(len(results))       
        # Increment the count of daily results
        count_total_results += len(results)     
        # Stop condition: all daily information has been imported
        if count_total_results >= sales_data['paging']['total']:
            print(f'** Request for day {start_request_date} has been processed. **')
            break       
        # Update the offset
        offset += limit
        time.sleep(1)  # To avoid hitting rate limits   

    return sum(all_sales), all_responses



async def batch_process_details(session, item_ids, url_item_func, url_variation_func, headers,
                        storage, bucket_name, date_blob_item_basic_path, date_variation_blob_path, 
                        chunk_size=100, semaphore_limit=100):
    """
    Processes API requests in batches and stores the responses for item details and variations in Google Cloud Storage using CloudStorage class.

    Args:
        session: The aiohttp session.
        item_ids (list): List of item IDs to process.
        url_item_func (callable): Function to generate the item details URL.
        url_variation_func (callable): Function to generate the item variation URL.
        headers (dict): Headers for the API requests.
        storage (CloudStorage): Instance of CloudStorage class for interacting with GCS.
        bucket_name (str): Name of the bucket where the files will be stored.
        date_blob_item_basic_path (str): Path for saving item details.
        date_variation_blob_path (str): Path for saving item variations.
        chunk_size (int, optional): Size of the chunks for batch processing. Defaults to 100.
        semaphore_limit (int, optional): Limit for concurrent requests. Defaults to 100.

    Returns:
        None
    """
    semaphore = asyncio.Semaphore(semaphore_limit)
    chunks = [item_ids[i:i + chunk_size] for i in range(0, len(item_ids), chunk_size)]
 
    async def process_chunk_details(chunk, batch_number):
        batch_responses_details = []
        batch_responses_variations = []
        timezone_offset = "-03:00"
        
        tasks = [
            fetch_details(session, item_id, batch_responses_details, batch_responses_variations,
                  url_item_func, url_variation_func, headers, semaphore) 
            for item_id in chunk
        ]
        
        await asyncio.gather(*tasks)
        
        # Save item details
        if batch_responses_details:
            process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f{timezone_offset}")
            item_details_fn = f'batch_id={batch_number}__total_items={len(chunk)}__process_time={process_time}.json'
            blob_items_prefix = date_blob_item_basic_path + item_details_fn
            storage.upload_json(bucket_name, blob_items_prefix, batch_responses_details)
        
        # Save item variations
        if batch_responses_variations:
            variation_details_fn = f'batch_id={batch_number}__total_variations={len(batch_responses_variations)}__process_time={process_time}.json'
            blob_variations_prefix = date_variation_blob_path + variation_details_fn
            storage.upload_json(bucket_name, blob_variations_prefix, batch_responses_variations)

    for batch_number, chunk in enumerate(chunks):
        await process_chunk_details(chunk, batch_number)
        await asyncio.sleep(1)  # Sleep to avoid hitting rate limits

async def fetch_details(session, item_id, batch_responses_details, batch_responses_variations, 
                url_item_func, url_variation_func, headers, semaphore):
    """
    Fetches item details and variations.

    Args:
        session: The aiohttp session.
        item_id (str): The item ID to be processed.
        batch_responses_details (list): List to store item details responses.
        batch_responses_variations (list): List to store item variations responses.
        url_item_func (callable): Function to generate the item details URL.
        url_variation_func (callable): Function to generate the item variation URL.
        headers (dict): Headers for the API requests.
        semaphore (asyncio.Semaphore): Semaphore to control the number of concurrent requests.

    Returns:
        None
    """
    async with semaphore:
        # Fetch item details
        async with session.get(url_item_func(item_id), headers=headers) as response:
            if response.status == 200:
                json_response = await response.json()
                batch_responses_details.append(json_response)
                
                # Check if item has variations
                if not extract_seller_sku(json_response.get('attributes', [])):  # Assume variations if no SKU
                    item_variations = []
                    for var in json_response.get('variations', []):
                        variation_id = var['id']
                        async with session.get(url_variation_func(item_id, variation_id), headers=headers) as variation_response:
                            if variation_response.status == 200:
                                variation_json = await variation_response.json()
                                item_variations.append(variation_json)
                    
                    batch_responses_variations.extend(item_variations)

