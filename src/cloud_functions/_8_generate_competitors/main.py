from src.cloud_functions._8_generate_competitors import functions
from datetime import datetime
import logging
from src.common.bigquery_connector import BigQueryManager
from src.config import settings

bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)

# Tabelas no bigquery
table_groups = 'datalake-v2-424516.datalake_v2.group_items'
table_competitors = 'datalake-v2-424516.datalake_v2.competitors_suggestions_mshops'


def generate_competitors_main(request):
    
    """
    Execução principal do script.
    """

    # Obter a data atual para o nome do arquivo
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Carregar e processar os dados de vendas
    selected_items_df = functions.load_and_process_sales_data()
    selected_items_df['correspondent_date'] = current_date

    # Salva os dados no bigquery
    bigquery.delete_existing_data(table_groups, seller_id=None, date=current_date)
    bigquery.match_dataframe_schema(selected_items_df, table_groups)
    bigquery.insert_dataframe(selected_items_df, table_groups)
    logging.info(f"Dados dos grupos de items exportados para bigquery: '{table_groups}'.")

    # Definir o número de itens top N por seller_id
    top_n = 20  # Ajuste conforme necessário

    # Obter os concorrentes para os top N itens
    concorrentes_topX_df = functions.get_competitors_for_top_n(selected_items_df.iloc, top_n=top_n)
    concorrentes_topX_df['correspondent_date'] = current_date

    # Verificar se o DataFrame não está vazio
    if not concorrentes_topX_df.empty:
        # Adicionar a coluna de data, se ainda não estiver presente
        if 'data' not in concorrentes_topX_df.columns:
            concorrentes_topX_df['data'] = datetime.now().strftime('%Y-%m-%d')

        # Salva os dados no bigquery
        bigquery.delete_existing_data(table_competitors, seller_id=None, date=current_date)
        bigquery.match_dataframe_schema(concorrentes_topX_df, table_competitors)
        bigquery.insert_dataframe(concorrentes_topX_df, table_competitors)
        logging.info(f"Dados dos concorrentes exportados para bigquery: '{table_competitors}'.")

    else:
        logging.info("Nenhum dado para exportar.")

    return 'Success', 200
