from google.cloud import bigquery
from google.cloud import secretmanager
import os
import pandas as pd
import pandas_gbq

class BigQueryManager:
    def __init__(self, credentials_path=None, secret_id=None):
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id= 'service_acount_dalaka_v2'):
        try:
            # Autenticação usando o arquivo local de credenciais
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        except:
            # Autenticação usando o Secret Manager
            secret_client = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/datalake-v2-424516/secrets/{secret_id}/versions/latest"
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
        pandas_gbq.to_gbq(table_id, if_exists='append')
        print(f'Data inserted into {table_id}.')
    
    def delete_existing_data(self, table_id, seller_id, date):
        query = f"""
        DELETE FROM {table_id}
        WHERE seller_id = {seller_id}
        AND date(correspondent_date) = '{date}'
        """
        self.run_query(query)
        print(f'Existing data deleted from {table_id} for date {date} and seller_id {seller_id}.')

    def get_list_dates_to_process(self, seller_id, table_management, table_to_process):
        query = f"""
            SELECT DISTINCT process_date
            FROM {table_management}
            WHERE
                1=1
                AND seller_id = {seller_id}
                AND table_name = '{table_to_process}'
                AND processed_to_bq = false
                  """
        df=self.run_query(query)
        list_dates = df['process_date'].to_list()
        return list_dates
    
    def update_logs_table(self, seller_id, date, destiny_table, management_table):

        query =  f"""
                UPDATE {management_table}
                SET processed_to_bq = true,
                    last_bq_processing = CURRENT_TIMESTAMP()
                WHERE 1=1
                AND seller_id = {seller_id}
                AND process_date = '{date}'
                AND processed_to_bq = false
                AND table_name = '{destiny_table}'
                """
        self.run_query(query)


