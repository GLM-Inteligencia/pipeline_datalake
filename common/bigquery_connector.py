from google.cloud import bigquery
from google.cloud import secretmanager
import os
import pandas as pd

class BigQueryManager:
    def __init__(self, credentials_path=None, secret_id=None):
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id):
        if credentials_path:
            # Autenticação usando o arquivo local de credenciais
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        elif secret_id:
            # Autenticação usando o Secret Manager
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/YOUR_PROJECT_ID/secrets/{secret_id}/versions/latest"
            response = secret_client.access_secret_version(request={"name": secret_name})
            credentials_json = response.payload.data.decode("UTF-8")
            with open("temp_credentials.json", "w") as cred_file:
                cred_file.write(credentials_json)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"

        return bigquery.Client()

    def run_query(self, query):
        query_job = self.client.query(query)
        return query_job.result().to_dataframe()

    def insert_dataframe(self, df, table_id):
        df.to_gbq(table_id, if_exists='append')
        print(f'Data inserted into {table_id}.')
    
    def delete_existing_data(self, table_id, seller_id, date):
        query = f"""
        DELETE FROM {table_id}
        WHERE seller_id = {seller_id}
        AND date(correspondent_date) = '{date}'
        """
        self.run_query(query)
        print(f'Existing data deleted from {table_id} for date {date} and seller_id {seller_id}.')
