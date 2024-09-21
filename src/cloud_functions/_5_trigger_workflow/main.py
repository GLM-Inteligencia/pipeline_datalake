from google.cloud import firestore
from google.cloud import workflows_v1
from google.cloud.workflows.executions_v1 import ExecutionsClient
import json
import requests
import traceback
from config import firestore_collection_users, project_id_firebase, project_id_workflow, location, workflow_name

from functions import read_firestore_data, get_seller_id_and_store_name, trigger_workflow


def triggers_workflow(request):

    users_dict = read_firestore_data(firestore_collection_users, project_id_firebase)

    for user, ids in users_dict.items():

        print(f' *** Treating data user : {user} ***')
        client_id = ids['client_id']
        client_secret = ids['client_secret']
        access_token = ids['access_token']

        if access_token == "":
            access_token = None

        else:
            url = "https://api.mercadolibre.com/oauth/token"

            payload = {
                "grant_type": "refresh_token",
                "client_id": f"{client_id}",
                "client_secret": f"{client_secret}",
                "refresh_token": f"{access_token}"
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            response = requests.post(url, data=payload, headers=headers)
            tokens = response.json()
            access_token = tokens.get("access_token")
            new_refresh_token = tokens.get("refresh_token")
            print(f'Refresh token: {new_refresh_token}')

        try:
            store_name, seller_id = get_seller_id_and_store_name(client_id, client_secret, access_token)
            print(f'Ids found for user: {user}. Store -> {store_name}')
            params = {
                'client_id': client_id,
                'client_secret': client_secret,
                'store_name': store_name.lower(),
                'seller_id': seller_id,
                'access_token': access_token
            }
            print(f'Lauching Workflow for user {user}')
            trigger_workflow(params, project_id_workflow, location, workflow_name)

        except Exception as e:
            print(f'Error encountered for user {user}: {str(e)}')
            traceback.print_exc()  # This will print the full traceback of the error


    return ('Success!', 200)
