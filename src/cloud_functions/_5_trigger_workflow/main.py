from google.cloud import firestore
from google.cloud import workflows_v1
from google.cloud.workflows.executions_v1 import ExecutionsClient
from google.cloud.workflows.executions_v1.types import Execution
import json
import requests
import traceback
import time
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
from src.common.trigger_cloud_function import TriggerCloudFunction
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import time


firestore_collection_users = 'users_credentials'
project_id_firebase = 'datalake-meli-dev'
project_id_workflow = "datalake-v2-424516"
location = "southamerica-east1"
workflow_name = "workflow-functions-datalakev2"


def triggers_workflow(request):

    users_dict = read_firestore_data(firestore_collection_users, project_id_firebase)
    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    trigger_function = TriggerCloudFunction(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    
    execution_ids = []

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
            print(f'Launching Workflow for user {user}')
            execution_id = trigger_workflow(params, project_id_workflow, location, workflow_name)
            execution_ids.append(execution_id)
        except Exception as e:
            print(f'Error encountered for user {user}: {str(e)}')
            traceback.print_exc()  # This will print the full traceback of the error

    # Now, wait until all workflows are done
    client = ExecutionsClient()
    execution_states = {}
    max_wait_time = 1800  # Maximum time to wait in seconds
    wait_interval = 30    # Interval between checks in seconds
    total_wait_time = 0

    while True:
        print("** Checking if workflows are still running **")
        all_done = True
        for execution_name in execution_ids:
            if execution_name not in execution_states or execution_states[execution_name] not in [Execution.State.SUCCEEDED, Execution.State.FAILED, Execution.State.CANCELLED]:
                execution = client.get_execution(name=execution_name)
                execution_states[execution_name] = execution.state
                state_name = Execution.State(execution.state).name
                print(f"Execution {execution_name} state: {state_name}")
                if execution.state not in [Execution.State.SUCCEEDED, Execution.State.FAILED, Execution.State.CANCELLED]:
                    all_done = False
        if all_done or total_wait_time >= max_wait_time:
            break
        time.sleep(wait_interval)
        total_wait_time += wait_interval

    # Now all workflows are done or maximum wait time exceeded
    if not all_done:
        print("Warning: Not all workflows completed within the maximum wait time.")
    else:
        print("All workflows have completed.")

    # Cleaning table model
    # print('Creating model sales') 
    # bigquery.run_query('delete from datalake-v2-424516.models.p_predictions_forecast where prediction_date = current_date()')  # Pausamos esse modelo por enquanto

    # Starting pipeline model sales
    # bigquery.run_query('CALL `datalake-v2-424516.datalake_v2.run_queries_sequentially`();')    # Pausamos esse modelo por enquanto

    print('Getting history sales')
    # Trigger function to calculate history sales
    trigger_function.trigger_function(function_url='https://southamerica-east1-datalake-v2-424516.cloudfunctions.net/get_max_sales_history',
                                           params= {}) 
    
    print('Creating frontend tables')
    # Creating frontend tables
    bigquery.run_query('CALL `datalake-v2-424516.datalake_v2.create_frontend_tables`();')

    print('Getting sellers information')
    # Trigger function to calculate history sales
    trigger_function.trigger_function(function_url='https://southamerica-east1-datalake-v2-424516.cloudfunctions.net/get_sellers_information',
                                           params= {}) 
    
    # Send frontend tables to mysql
    tables_list = ['competitors', 'general', 'performance_table', 'stock_seller', 'suggested_items']

    for table in tables_list:
        df= bigquery.run_query(f'select * from datalake-v2-424516.tables_frontend.{table}')

        memory_usage = df.memory_usage(deep=True).sum()/ (1024 ** 2)
        print(f"Tabela: {table} / Tamanho em memória: {memory_usage:.2f} MB" )

        start_time = time.time()
        upload_data_to_mysql(df, table_name= f'{table}_v1')
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Tempo decorrido: {elapsed_time:.2f} segundos")
        print('-----------------------------------')
        
        return ('Success!', 200)



def upload_data_to_mysql(df, table_name):
    # Replace pandas.NA and np.nan with None
    df = df.where(pd.notnull(df), None)
    
    password = quote_plus('Glm@mysql24')  # Your actual password

    # Create the SQLAlchemy engine
    engine = create_engine(f'mysql+pymysql://geraldo-papa:{password}@34.123.250.92/glm')

    # Use the 'replace' method to drop the table if it exists and create a new one
    # Alternatively, use 'append' to add data to the existing table
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False, chunksize=1000, method='multi')

    print("Data uploaded!!!")


def trigger_workflow(parameters, project_id, location, workflow_name):

    # Create a client
    client = ExecutionsClient()

    # Construct the fully qualified location path
    workflow_path = f"projects/{project_id}/locations/{location}/workflows/{workflow_name}"

    # Execute the workflow
    execution = client.create_execution(
        parent=workflow_path,
        execution={"argument": json.dumps(parameters)}
    )

    # Return the execution name (ID)
    return execution.name  # The execution name is in the format projects/.../executions/...

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
