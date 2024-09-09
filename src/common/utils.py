import asyncio
import json
from datetime import datetime
import requests 

async def batch_process(session, items, url_func_or_string, headers, 
                        bucket_name, date_blob_path, storage, 
                        chunk_size=100, add_item_id=False, params=None):
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
        params (dict, optional): Additional parameters to pass in the request.

    Returns:
        None
    """
    semaphore = asyncio.Semaphore(100)
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    async def process_chunk(chunk, batch_number):

        if params:
            tasks = [fetch(session, item, url_func_or_string, headers, semaphore, add_item_id, param) for item, param in zip(chunk, params)]
        else:
            tasks = [fetch(session, item, url_func_or_string, headers, semaphore, add_item_id) for item in chunk]

        responses = await asyncio.gather(*tasks)

        if responses:
            # Save batch responses to Cloud Storage
            process_time = datetime.now().strftime(f"%Y-%m-%dT%H:%M:%M.%f-03:00")
            filename = f'batch_{batch_number}__process_time={process_time}.json'
            storage.upload_json(bucket_name, f'{date_blob_path}{filename}', responses)

    for batch_number, chunk in enumerate(chunks):
        await process_chunk(chunk, batch_number)
        await asyncio.sleep(1)  # Sleep to avoid rate limits

async def fetch(session, item_id, url_func_or_string, headers, semaphore, add_item_id, params=None):
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
            url = url_func_or_string(item_id)  # Call the URL function with just item_id
        else:
            url = url_func_or_string  # Fixed URL string
        
        # Make the request with params (if provided)
        async with session.get(url, headers=headers, params=params) as response:
            if response.status == 200:
                data = await response.json()  # Await the json() method
                if add_item_id:
                    data['item_id'] = item_id
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
                        if value is not None:  # Filter out None values
                            items_list.append(value)
                    else:
                        # Multiple keys: Create a dictionary of key-value pairs
                        item_dict = {key: item.get(key, None) for key in key_names}
                        if all(value is not None for value in item_dict.values()):  # Ensure all values are not None
                            items_list.append(item_dict)

    # Return a list of unique values if it's a single key, otherwise list of dictionaries
    return list(set(items_list)) if is_single_key else [dict(t) for t in {tuple(d.items()) for d in items_list}]


def fetch_all_items(url, access_token, seller_id):
    offset = 0
    page = 1  # New page parameter
    max_offset = 1000  # MercadoLibre API max offset
    count_total_results = 0
    all_products = []
    all_responses = []  # List to store all the JSON responses
    limit = 50 # Number of products per request

    params = {
      'access_token': access_token,
      'limit': limit,
      }

    while True:
        if offset >= max_offset:
            # Switch to pagination mode
            params['page'] = page
            if 'offset' in params:
                del params['offset']  # Remove offset if using page
            print(f"Switching to pagination at page {page}")
        else:
            # Use offset for first 1000 items
            params['offset'] = offset

        # request api
        response = requests.get(url(seller_id), params=params)

        # checking request status
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.json()}")

        # getting items data
        items_data = response.json()
        results = items_data['results']
        print(f"Offset {offset} (or page {page}): {len(results)} items")

        # Add the entire JSON response to the list
        all_responses.append(items_data)

        # increment count of daily results
        count_total_results += len(results)

        # Add the current batch of product IDs to the all_products list
        products = items_data.get('results', [])
        all_products.extend(products)

        # stop condition -> all daily information has been imported
        if count_total_results >= items_data['paging']['total']:
            print(f'** Request items have been processed. **')
            break

        # updating offset and page
        if offset < max_offset:
            offset += limit
        else:
            page += 1

    print(f'** Number of items found : {len(all_products)}')
    
    return all_products, all_responses


