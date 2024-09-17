from google.cloud import bigquery
from google.cloud import secretmanager
import os
import pandas as pd
import pandas_gbq

class BigQueryManager:
    def __init__(self, credentials_path=None, secret_id= 'service_acount_dalaka_v2'):
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id):
        try:
            # Check if credentials_path is provided and exists
            if credentials_path and os.path.exists(credentials_path):
                # Authentication using the local credentials file
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                print(f"Using local credentials from: {credentials_path}")
            else:
                # Fallback to authentication using Secret Manager
                print("Fetching credentials from Secret Manager.")
                secret_client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/datalake-v2-424516/secrets/{secret_id}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_name})
                credentials_json = response.payload.data.decode("UTF-8")
    
                # Write credentials to a temporary file and set the environment variable
                with open("temp_credentials.json", "w") as cred_file:
                    cred_file.write(credentials_json)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"
                print("Using credentials from Secret Manager.")
            
        except Exception as e:
            raise RuntimeError(f"Error during authentication: {str(e)}")
    
        # Return the BigQuery client
        return bigquery.Client()


    def run_query(self, query):
        query_job = self.client.query(query)
        return query_job.result().to_dataframe()

    def insert_dataframe(self, df, table_id):
        pandas_gbq.to_gbq(df, table_id, if_exists='append')
        print(f'Data inserted into {table_id}.')
    
    def delete_existing_data(self, table_id, seller_id, date, date_filter_name = 'correspondent_date'):
        
        if isinstance(date, list):
            
            query = f"""
            DELETE FROM {table_id}
            WHERE seller_id = {seller_id}
            AND date({date_filter_name}) in ({tuple(date)})
            """

        else:
            query = f"""
            DELETE FROM {table_id}
            WHERE seller_id = {seller_id}
            AND date({date_filter_name}) = '{date}'
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

        if isinstance(date, list):
            
            query = f"""
                    UPDATE {management_table}
                    SET processed_to_bq = true,
                        last_bq_processing = CURRENT_TIMESTAMP()
                    WHERE 1=1
                    AND seller_id = {seller_id}
                    AND process_date in ({tuple(date)})
                    AND processed_to_bq = false
                    AND table_name = '{destiny_table}'
                    """

        else:
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

    def match_dataframe_schema(self, df, table_id):
        """
        Modifies the DataFrame to match the schema of the BigQuery table (table_id).
        
        Parameters:
        df (pd.DataFrame): The DataFrame to be matched with the BigQuery table schema.
        table_id (str): The BigQuery table identifier (project_id.dataset_id.table_id).
        
        Returns:
        pd.DataFrame: A DataFrame with the adjusted schema.
        """
        # Fetch the schema of the target table from BigQuery
        table = self.client.get_table(table_id)
        schema = table.schema
        
        # Create a dictionary to store the column names and their corresponding types
        schema_dict = {}
        for field in schema:
            schema_dict[field.name] = field.field_type
        
        # Adjust DataFrame columns to match the BigQuery table schema
        for column, dtype in schema_dict.items():
            if column in df.columns:
                if dtype == 'STRING':
                    df[column] = df[column].astype(str)
                elif dtype == 'INTEGER':
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype('Int64')
                elif dtype == 'FLOAT':
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                elif dtype == 'BOOLEAN':
                    df[column] = df[column].astype(bool)
                elif dtype == 'TIMESTAMP':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
        
        print("Schema adjusted to match BigQuery table.")
        return df


