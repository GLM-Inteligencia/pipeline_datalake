
from datetime import datetime, timedelta
from src.common.bigquery_connector import BigQueryManager
import pandas as pd
import pytz
from src.config import settings


def get_max_sales_history(request):
  
  bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
  
    # Get salles table from BigQuery
  df_sales = get_sales_table(settings.TABLE_ORDERS, bigquery)

  # Create max 7 and 21 days transactions per SKU
  df_max_sales = create_max_7_and_21_days_history_per_sku(df_sales)

  # Get past 7 days sales
  df_past_7_days_sales = create_past_sales(df_sales, 7)
  
  # Get past 21 days sales
  df_past_21_days_sales = create_past_sales(df_sales, 21)

  # Merge tables
  df_consolidado = df_max_sales.merge(df_past_7_days_sales, on = ['seller_sku', 'seller_id'], how = 'left').merge(df_past_21_days_sales, on = ['seller_sku', 'seller_id'], how = 'left')

  # BigQuery dataset and table ID
  dataset_table_id = settings.TABLE_PREDICTED_SALES
  
  # Insert into BigQuery
  bigquery.insert_dataframe(df_consolidado, dataset_table_id)

  return ("Concurrents predicted sales information processed successfully.", 200)


def get_sales_table(table_id, bigquery):

  query = f'''
  SELECT date_created, seller_sku, seller_id, quantity FROM {table_id}
  WHERE payment_status = 'approved'
  '''

  # Crie um job de consulta para extrair todos os dados da tabela
  df = bigquery.run_query(query)

  return df.set_index('date_created')

def create_max_7_and_21_days_history_per_sku(df_sales):

  # This will hold the max quantity sold in a week for each product
  max_quantity_info  = []

  # Process each product
  for (item_id, seller_id), group in df_sales.groupby(['seller_sku', 'seller_id']):
    
      num_days = df_sales.loc[df_sales['seller_sku'] == item_id].groupby('seller_sku')['quantity'].resample('D').sum().count()
            
      if num_days >= 7:
          # Resample to daily data, filling missing days with 0, then compute a 7-day rolling window sum
          rolling_sum_7 = group['quantity'].resample('D').sum().rolling(window=7).sum()
      else:
          rolling_sum_7 = group['quantity'].resample('D').sum().rolling(window=num_days).sum()
        
      if num_days >= 21:
          rolling_sum_21 = group['quantity'].resample('D').sum().rolling(window=21).sum()
      else:
          rolling_sum_21 = group['quantity'].resample('D').sum().rolling(window=num_days).sum()

      max_quantity_7 = rolling_sum_7.max()
      max_quantity_21 = rolling_sum_21.max()
    
      max_date_7 = rolling_sum_7.idxmax(skipna=True)  
      max_date_21 = rolling_sum_21.idxmax(skipna=True)

      # Append the results for this product to the list
      max_quantity_info.append({'seller_sku': item_id,
                                'seller_id': seller_id,
                                'max_sold_7_days_history': max_quantity_7,
                                'date_end_max_sold_history_7_days': max_date_7,
                                'max_sold_21_days_history': max_quantity_21,
                                'date_end_max_sold_history_21_days': max_date_21
                               
                               })

  # Convert the list of dicts to a DataFrame for nicer display
  max_quantity_df = pd.DataFrame(max_quantity_info)

  max_quantity_df['insertion_date'] = datetime.now()

  return max_quantity_df


def create_past_sales(df_sales, num_days):
    # Set the timezone for UTC-4 (Eastern Time)
    est = pytz.timezone('America/New_York')
    
    # Get yesterday's date in UTC-4 and set to 23:59:59
    end_date = datetime.now(est).replace(hour=23, minute=59, second=59) - timedelta(days=1)
    
    # Calculate the start of the date range (num_days before the end_date)
    start_date = end_date - timedelta(days=num_days - 1)
    start_date = start_date.replace(hour=0, minute=0, second=0)
    
    # Convert start_date and end_date to UTC for comparison with dataframe index
    start_date_utc = start_date.astimezone(pytz.utc)
    end_date_utc = end_date.astimezone(pytz.utc)
    
    # Filter the DataFrame for the given date range
    df_past_sales = df_sales.loc[(df_sales.index >= start_date_utc) & (df_sales.index <= end_date_utc)]
    
    # Group by 'seller_sku' and sum the quantities
    df_past_sales = df_past_sales.groupby(['seller_sku', 'seller_id'], as_index=False).agg({'quantity': 'sum'}).rename({'quantity': f'past_{num_days}_sales'}, axis=1)

    print(f'Start date : {start_date}')
    print(f'End date : {end_date}')
    return df_past_sales

