from datetime import datetime
import json
import logging
import requests
from src.common.bigquery_connector import BigQueryManager
from src.config import settings
import pandas as pd

def main_fetch_sellers_information(request):

    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    table_id = settings.TABLE_SELLER_INFORMATION

    # Getting list of sellers to update
    query = """
    WITH sellers_ids AS (
        SELECT DISTINCT competitor_seller_id
        FROM `datalake-v2-424516.datalake_v2.items_competitors_catalog`

        UNION ALL

        SELECT DISTINCT competitor_seller_id
        FROM `datalake-v2-424516.datalake_v2.items_competitors_details`
    )

    SELECT DISTINCT si.competitor_seller_id
    FROM sellers_ids si 
    LEFT JOIN datalake-v2-424516.datalake_v2.update_sellers_competitors_details sc
    ON CAST(sc.competitor_seller_id AS INT64) = si.competitor_seller_id
    WHERE sc.competitor_seller_id IS NULL
    """

    sellers_df = bigquery.run_query(query)
    sellers_list = sellers_df['competitor_seller_id'].to_list()

    print(f'{len(sellers_list)} novos sellers para processar')

    if len(sellers_list) == 0:
        return 'Zero novos sellers para processar', 200
    
    else:
        seller_details_list = []
        for seller_id in sellers_list:
            details = fetch_seller_details(seller_id)
            seller_details_list.append(details)

        # Creates a dataframe with all the information
        print('Creating dataframe')
        df_to_save = product_to_save(seller_details_list)

        print(f'{df_to_save.shape[0]} sellers encontrados')

        # Saving dataframe
        print('Match schema dataframe')
        df_to_save = bigquery.match_dataframe_schema(df_to_save, table_id)

        print('Inserting dataframe')
        bigquery.insert_dataframe(df_to_save, table_id)

    return 'Success', 200



def fetch_seller_details(seller_id):

    url = f"https://api.mercadolibre.com/users/{seller_id}"

    response = requests.get(url)
    response.raise_for_status()
    seller_data = response.json()

    return seller_data
    

def product_to_save(product_details_list):
    competitor_seller_list = []
    process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for product_data in product_details_list:
        if product_data:  

            seller_reputation = product_data.get("seller_reputation", {})
            transactions = seller_reputation.get("transactions", {})
            site_status = product_data.get("status",{})

            product_dict = {
                'process_time': process_time,
                "competitor_seller_id": product_data.get("id"),
                "competitor_seller_nickname": product_data.get("nickname"),
                "competitor_seller_level_id": seller_reputation.get("level_id", ""),
                "competitor_power_seller_status": seller_reputation.get("power_seller_status", ""),
                "competitor_transactions_period": transactions.get("period", ""),
                "competitor_transactions_total": transactions.get("total", 0),
                "competitor_site_status": site_status.get("site_status", ""), 
                "competitor_permalink": product_data.get("permalink")
            }
            competitor_seller_list.append(product_dict)

    return pd.DataFrame(competitor_seller_list)
