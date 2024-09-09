import os

import functions_framework
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from flask import jsonify
from datetime import datetime
import io

@functions_framework.http
def store_import_data(request):
    """
    Cloud Function that receives a Cloud Storage path of a CSV file, lists files inside the folder,
    selects the most recent CSV (assuming files are named by dates), and loads the data into BigQuery.
    """
    try:
        # Extract the CSV file path from the HTTP request
        request_json = request.get_json()

        if not request_json:
            return jsonify({'error': 'Invalid request: must provide input_type and store_identifier in the request body.'}), 400

        input_type = request_json.get('input_type')
        store_identifier = request_json.get('store_identifier')
        seller_id = request_json.get('seller_id')

        if not input_type or not store_identifier or not seller_id:
            return jsonify({'error': 'Missing required parameters: input_type, store_identifier, and seller_id.'}), 400

        # Parse the bucket and the directory
        bucket_name = 'glm-store'
        directory_prefix = f'{store_identifier}/inputs/{input_type}/'

        # Initialize Cloud Storage client
        storage_client = storage.Client()

        try:
            bucket = storage_client.bucket(bucket_name)
        except NotFound:
            return jsonify({'error': f'Bucket {bucket_name} not found.'}), 404

        # List all files in the folder
        blobs = bucket.list_blobs(prefix=directory_prefix)
        date_files = []

        # Collect files named as dates (assuming file names are ISO format or sortable date strings)
        for blob in blobs:
            filename = blob.name.replace(directory_prefix, '')
            date_str = filename.split('/')[0]
            print(f'Found file: {filename} with date: {date_str}')
            if filename.endswith('.csv'):
                try:
                    # Parse filename as a date
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                    date_files.append((date, blob.name))
                except ValueError:
                    print(f'Skipping file {filename}, not a valid date format')
                    continue  # Ignore files not following date naming

        if not date_files:
            return jsonify({'error': f'No valid CSV files found in {directory_prefix}.'}), 404

        # Sort files by date and pick the most recent one
        latest_date, latest_file_path = max(date_files, key=lambda x: x[0])
        print(f'Latest file selected: {latest_file_path}')

        # Now latest_file_path holds the most recent file name (full GCS path)
        blob = bucket.blob(latest_file_path)

        # Download the CSV file as a string
        try:
            csv_data = blob.download_as_text()
            print(f'CSV data length: {len(csv_data)} characters')
        except Exception as e:
            return jsonify({'error': f'Error downloading CSV file: {e}'}), 500

        # Convert CSV to pandas DataFrame
        try:
            df = pd.read_csv(io.StringIO(csv_data), sep=';')
            df['seller_id'] = seller_id
            df['process_time'] = datetime.now()
            print(f'DataFrame created with {len(df)} rows')
        except Exception as e:
            return jsonify({'error': f'Error reading CSV: {e}'}), 500

        # Initialize BigQuery client
        bigquery_client = bigquery.Client()

        # Generate the table name dynamically based on input_type
        table_name = f'datalake-v2-424516.inputs.{input_type}'
        print(f'Target BigQuery table: {table_name}')

        # Load the DataFrame to BigQuery
        try:
            job = bigquery_client.load_table_from_dataframe(
                df, table_name, job_config=bigquery.LoadJobConfig(autodetect=True)
            )
            job.result()  # Wait for the job to complete
            print(f'BigQuery job completed successfully')
        except Exception as e:
            return jsonify({'error': f'Error uploading data to BigQuery: {e}'}), 500

        return jsonify({'message': f'Successfully uploaded {len(df)} rows from {latest_file_path} to BigQuery.'}), 200

    except Exception as e:
        # Catch any unexpected error and return the message
        import traceback
        traceback_str = traceback.format_exc()
        print(f'Unexpected error: {traceback_str}')
        return jsonify({'error': str(e), 'trace': traceback_str}), 500
