from google.cloud import bigquery
from google.cloud import secretmanager
import os
import pandas as pd
import pandas_gbq
from google.api_core.exceptions import NotFound

class BigQueryManager:
    def __init__(self, credentials_path=None, secret_id='service_acount_dalaka_v2'):
        self.client = self.authenticate(credentials_path, secret_id)

    def authenticate(self, credentials_path, secret_id):
        try:
            if credentials_path and os.path.exists(credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                print(f"Using local credentials from: {credentials_path}")
            else:
                print("Fetching credentials from Secret Manager.")
                secret_client = secretmanager.SecretManagerServiceClient()
                secret_name = f"projects/datalake-v2-424516/secrets/{secret_id}/versions/latest"
                response = secret_client.access_secret_version(request={"name": secret_name})
                credentials_json = response.payload.data.decode("UTF-8")

                with open("temp_credentials.json", "w") as cred_file:
                    cred_file.write(credentials_json)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "temp_credentials.json"
                print("Using credentials from Secret Manager.")
                
        except Exception as e:
            raise RuntimeError(f"Error during authentication: {str(e)}")
        
        return bigquery.Client(location='southamerica-east1')

    def table_exists(self, table_id):
        try:
            self.client.get_table(table_id)
            return True
        except NotFound:
            print(f"Tabela {table_id} não encontrada.")
            return False

    def run_query(self, query):
        try:
            query_job = self.client.query(query)
            return query_job.result().to_dataframe()
        except Exception as e:
            print(f"Erro ao executar a consulta: {str(e)}")

    def insert_dataframe(self, df, table_id):
        if self.table_exists(table_id):
            pandas_gbq.to_gbq(df, table_id, if_exists='append')
            print(f'Data inserted into {table_id}.')
        else:
            print(f"Tabela de destino {table_id} não existe. Não foi possível inserir os dados.")

    def delete_existing_data(self, table_id, seller_id, date, date_filter_name='correspondent_date'):
        query = f"""
            DELETE FROM {table_id}
            WHERE seller_id = {seller_id}
            AND date({date_filter_name}) {self.build_date_condition(date)}
            """
        self.run_query(query)
        print(f'Existing data deleted from {table_id} for dates {date} and seller_id {seller_id}.')

    def build_date_condition(self, date):
        if isinstance(date, list) and len(date) > 1:
            return f"in {tuple(date)}"
        if isinstance(date, list):
            date = date[0]
        return f"= '{date}'"

    def get_list_dates_to_process(self, seller_id, table_management, table_to_process):
        query = f"""
            SELECT DISTINCT process_date
            FROM {table_management}
            WHERE seller_id = {seller_id} AND table_name = '{table_to_process}'
            AND processed_to_bq = false
        """
        df = self.run_query(query)
        return df['process_date'].to_list() if df is not None else []

    def update_logs_table(self, seller_id, date, destiny_table, management_table):
        query = f"""
            UPDATE {management_table}
            SET processed_to_bq = true, last_bq_processing = CURRENT_TIMESTAMP()
            WHERE seller_id = {seller_id} AND table_name = '{destiny_table}'
            AND date(process_date) {self.build_date_condition(date)}
            AND processed_to_bq = false
        """
        self.run_query(query)
        print(f'Logs table {management_table} updated for seller_id {seller_id} and dates {date}.')

    def match_dataframe_schema(self, df, table_id):
        try:
            table = self.client.get_table(table_id)
            schema_dict = {field.name: field.field_type for field in table.schema}
            
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
            return df[schema_dict.keys()]
        except Exception as e:
            print(f"Error matching schema: {str(e)}")
            return df

    def create_table(self, table_id, df_schema):
        if self.table_exists(table_id):
            print(f"A tabela {table_id} já existe.")
            return
        
        schema = [
            bigquery.SchemaField(
                column,
                'INTEGER' if dtype in ['int64', 'Int64'] else 'FLOAT' if dtype == 'float64' else
                'BOOLEAN' if dtype == 'bool' else 'TIMESTAMP' if dtype == 'datetime64[ns]' else 'STRING'
            )
            for column, dtype in df_schema.dtypes.items()
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        try:
            self.client.create_table(table)
            print(f"Tabela {table_id} criada com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela {table_id}: {str(e)}")


    def update_timezones_in_bigquery(self,destiny_table):

        query = f"""
        UPDATE `{destiny_table}`
        SET 
            date_created = TIMESTAMP(DATETIME(date_created, "America/Sao_Paulo")),
            date_approved = TIMESTAMP(DATETIME(date_approved, "America/Sao_Paulo")),
            date_last_modified = TIMESTAMP(DATETIME(date_last_modified, "America/Sao_Paulo")),
            date_closed = TIMESTAMP(DATETIME(date_closed, "America/Sao_Paulo")),
            date_last_updated = TIMESTAMP(DATETIME(date_last_updated, "America/Sao_Paulo"))
        WHERE TRUE;
        """
        
        query_job = self.client.query(query)  # Executa a consulta
        query_job.result()  # Aguarda a conclusão
        print("Timezones updated to São Paulo in BigQuery.")
        