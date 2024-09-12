from google.cloud import storage
import json
import os
from google.cloud import secretmanager

class CloudStorage:
    def __init__(self, credentials_path=None, secret_id='service_acount_dalaka_v2'):
        self.client = self.authenticate(credentials_path, secret_id)

import os
from google.cloud import storage
from google.cloud import secretmanager

class CloudStorage:
    def __init__(self, credentials_path=None, secret_id='service_acount_dalaka_v2'):
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id):
        try:
            # Check if credentials_path is provided and exists
            if credentials_path and os.path.exists(credentials_path):
                # Use local credentials if available
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                print(f"Using local credentials from: {credentials_path}")
            else:
                # Fallback to Secret Manager if credentials_path is not provided or invalid
                print("Fetching credentials from Secret Manager.")
                secret_client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/datalake-v2-424516/secrets/{secret_id}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_name})
                credentials_json = response.payload.data.decode("UTF-8")
                
                # Write credentials to a temporary file
                with open("temp_credentials.json", "w") as cred_file:
                    cred_file.write(credentials_json)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"
                print("Using credentials from Secret Manager.")
        
        except Exception as e:
            raise RuntimeError(f"Error during authentication: {str(e)}")
        
        return storage.Client()


    def upload_json(self, bucket_name, destination_blob_name, data):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        print(f'File uploaded to {destination_blob_name}.')

    def download_json(self, bucket_name, blob_name):
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()
        return json.loads(content)

    def list_blobs(self, bucket_name, prefix):
        bucket = self.client.bucket(bucket_name)
        return list(bucket.list_blobs(prefix=prefix))

    def clean_blobs(self, bucket_name, prefix):
        bucket = self.client.bucket(bucket_name)
        blobs = self.list_blobs(bucket_name, prefix)

        for blob in blobs:
            print(f"Deleting blob: {blob.name}")
            blob.delete()
        print(f"All blobs with prefix {prefix} have been deleted.")
