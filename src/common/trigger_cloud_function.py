import os
import requests
from google.cloud import secretmanager
from google.auth.transport.requests import Request
from google.oauth2 import id_token

class TriggerCloudFunction:
    def __init__(self, credentials_path=None, secret_id='service_acount_dalaka_v2'):
        self.credentials_path = credentials_path
        self.secret_id = secret_id
        self.authenticate()

    def authenticate(self):
        try:
            # Check if credentials_path is provided and exists
            if self.credentials_path and os.path.exists(self.credentials_path):
                # Use local credentials if available
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
                print(f"Using local credentials from: {self.credentials_path}")
            else:
                # Fallback to Secret Manager if credentials_path is not provided or invalid
                print("Fetching credentials from Secret Manager.")
                secret_client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/datalake-v2-424516/secrets/{self.secret_id}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_name})
                credentials_json = response.payload.data.decode("UTF-8")
                
                # Write credentials to a temporary file
                with open("temp_credentials.json", "w") as cred_file:
                    cred_file.write(credentials_json)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"
                print("Using credentials from Secret Manager.")
        
        except Exception as e:
            raise RuntimeError(f"Error during authentication: {str(e)}")

    def trigger_function(self, function_url, params):
        """
        Trigger the Cloud Function by sending an HTTP POST request to the function's URL.
        
        Args:
            function_url (str): The URL of the Cloud Function.
            params (dict): Parameters to send in the POST request.
        
        Returns:
            Response from the Cloud Function.
        """
        try:
            # Generate an identity token for the service account to authenticate the request
            audience = function_url  # The function's URL acts as the audience
            identity_token = id_token.fetch_id_token(Request(), audience)
            
            # Set headers including the generated ID token for authorization
            headers = {
                'Authorization': f'Bearer {identity_token}',
                'Content-Type': 'application/json'
            }

            # Send POST request to Cloud Function
            response = requests.post(function_url, json=params, headers=headers)
            return response

        except Exception as e:
            print(f'Bad response for function: {e}')
