import json
import google.auth
from google.auth.transport.requests import Request
import requests
from google.oauth2 import service_account
from src.config import settings

# Define the Cloud Function URL
cloud_function_url = 'https://southamerica-east1-datalake-v2-424516.cloudfunctions.net/inputs_storage_to_bq'

# Define the body payload with the required parameters
args = {
  "access_token": "APP_USR-2951712600123976-090703-2f7ae77293ddc16a4d9f5c687a90c3ed-2900565",
  "client_id": "2951712600123976",
  "client_secret": "QprAIl8ydXzcxFVHjnIHT6fUQ8KpzADV",
  "seller_id": 2900565,
  "store_name": "albacoa pescaesportiva"
}

# Specify the path to your service account JSON key file
service_account_path = settings.PATH_SERVICE_ACCOUNT

# Load the service account credentials
credentials = service_account.IDTokenCredentials.from_service_account_file(
    service_account_path,
    target_audience=cloud_function_url
)

# Refresh the credentials to obtain the token
auth_request = Request()
credentials.refresh(auth_request)

# Define the headers, including the authorization token
headers = {
    "Authorization": f"Bearer {credentials.token}",
    "Content-Type": "application/json"
}

# Make the HTTP POST request to trigger the Cloud Function
response = requests.post(cloud_function_url, headers=headers, json=args, timeout=1800)

response.json()