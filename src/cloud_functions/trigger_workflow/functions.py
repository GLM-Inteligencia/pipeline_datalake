from google.cloud import firestore
from google.cloud import workflows_v1
from google.cloud.workflows.executions_v1 import ExecutionsClient
import json
import requests


# Read data from Firestore
def read_firestore_data(collection_firestore, project_id):

    # Initialize the Firestore client
    client = firestore.Client(project=project_id)

    # Reference to your Firestore collection
    collection_ref = client.collection(collection_firestore)

    # Fetch all documents in the collection
    docs = collection_ref.stream()
    users_dict = {}

    for doc in docs:
        users_dict[doc.id] = doc.to_dict()
    
    return users_dict

def get_seller_id_and_store_name(client_id, client_secret, access_token):
    
    if not access_token:
        print("Getting access_token")
        token_url = 'https://api.mercadolibre.com/oauth/token'

        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }

        response = requests.post(token_url, data=token_data)
        token_info = response.json()
        access_token = token_info['access_token']
    
    # Step 2: Retrieve User Information
    user_info_url = 'https://api.mercadolibre.com/users/me'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    user_response = requests.get(user_info_url, headers=headers)
    user_info = user_response.json()
    
    # Extract seller ID and store name
    seller_id = user_info['id']
    store_name = user_info.get('nickname', 'N/A').split('.')[0]  # Using 'nickname' as store name

    return store_name, seller_id


def trigger_workflow(parameters, project_id, location, workflow_name):

    # Create a client
    client = ExecutionsClient()

    # Construct the fully qualified location path
    workflow_path = f"projects/{project_id}/locations/{location}/workflows/{workflow_name}"

    # Execute the workflow
    response = client.create_execution(
        parent=workflow_path,
        execution={"argument": json.dumps(parameters)}
    )
