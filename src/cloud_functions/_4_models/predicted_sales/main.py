import logging
from datetime import datetime, timedelta
from src.common.bigquery_connector import BigQueryManager
import pandas as pd
import pytz
from src.config import settings

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(name)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

def get_max_sales_history(request):
    logger.info("Starting the get_max_sales_history process")

    # Initialize BigQuery manager
    bigquery = BigQueryManager(credentials_path=settings.PATH_SERVICE_ACCOUNT)
    logger.info("BigQueryManager initialized")

    # Get sales data
    df_sales = get_sales_table(settings.TABLE_ORDERS, bigquery)
    logger.info(f"Loaded {len(df_sales)} sales records from BigQuery")

    # Compute rolling maxima per SKU + channel and globally
    logger.info("Computing maximum sales history per SKU and channel")
    df_max_sales = create_max_history_per_sku(df_sales, context_channel=True)
    logger.info("7 and 21 days max sales per SKU and channel computed")

    logger.info("Computing maximum sales history per SKU (global)")
    df_max_sales_global = create_max_history_per_sku(df_sales, context_channel=False)
    logger.info("7 and 21 days max sales per SKU (global) computed")

    # Compute past sales
    logger.info("Computing past 7 days sales")
    df_past_7_days_sales_with_channel, df_past_7_days_sales = create_past_sales(df_sales, 7)

    logger.info("Computing past 21 days sales")
    df_past_21_days_sales_with_channel, df_past_21_days_sales = create_past_sales(df_sales, 21)

    # Merge results
    logger.info("Merging all computed dataframes")
    # For global max, we do not have context_channel, so merge on just sku and seller_id first.
    df_consolidado = (
        df_max_sales
        .merge(df_past_21_days_sales_with_channel, on=['seller_sku', 'seller_id', 'context_channel'], how='left')
        .merge(df_past_7_days_sales_with_channel, on=['seller_sku', 'seller_id', 'context_channel'], how='left')
        .merge(df_max_sales_global, on=['seller_sku', 'seller_id'], how='left', suffixes=('', '_global'))
        .merge(df_past_21_days_sales, on=['seller_sku', 'seller_id'], how='left', suffixes=('', '_global'))
        .merge(df_past_7_days_sales, on=['seller_sku', 'seller_id'], how='left', suffixes=('', '_global'))
    )
    logger.info("Dataframes merged successfully")

    # Clear and insert into BigQuery
    dataset_table_id = settings.TABLE_PREDICTED_SALES
    logger.info(f"Clearing data from {dataset_table_id} for today's date")
    bigquery.run_query(f"DELETE FROM {dataset_table_id} WHERE DATE(insertion_date) = CURRENT_DATE()")

    logger.info("Inserting new data into BigQuery")
    bigquery.insert_dataframe(df_consolidado, dataset_table_id)
    logger.info("Data inserted successfully")

    return ("Concurrents predicted sales information processed successfully.", 200)


def get_sales_table(table_id, bigquery):
    query = f'''
    SELECT 
        date_created, 
        seller_sku, 
        seller_id, 
        context_channel, 
        quantity 
    FROM {table_id}
    WHERE payment_status = 'approved'
    '''
    df = bigquery.run_query(query)
    # Ensure datetime index
    df['date_created'] = pd.to_datetime(df['date_created'])
    df.set_index('date_created', inplace=True)
    return df


def create_max_history_per_sku(df_sales, context_channel=True):
    """
    Compute the max rolling 7-day and 21-day sums and their corresponding dates for each SKU.
    If context_channel=True, compute per SKU+seller_id+context_channel.
    If context_channel=False, compute per SKU+seller_id only (global).
    """

    group_cols = ['seller_sku', 'seller_id']
    if context_channel:
        group_cols.append('context_channel')

    # Sort index to ensure proper resampling
    df_sales = df_sales.sort_index()

    # Resample daily sums
    # This creates a DataFrame with daily frequency rows and sum of quantities per day.
    df_daily = (
        df_sales
        .groupby(group_cols)
        .resample('D')['quantity']
        .sum()
        .reset_index()
        .sort_values(group_cols + ['date_created'])
    )

    # Set multi-index for rolling operations
    df_daily.set_index(group_cols + ['date_created'], inplace=True)

    # Compute rolling sums
    rolling_7 = df_daily.groupby(group_cols)['quantity'].rolling(7, min_periods=1).sum()
    rolling_21 = df_daily.groupby(group_cols)['quantity'].rolling(21, min_periods=1).sum()

    # Compute max values
    max_7 = rolling_7.groupby(group_cols).max().reset_index(name='max_sold_7_days_history')
    max_21 = rolling_21.groupby(group_cols).max().reset_index(name='max_sold_21_days_history')

    # Identify the dates for the max values
    # idxmax returns the index labels at the max value
    idxmax_7 = rolling_7.groupby(group_cols).idxmax().reset_index(name='idx_7')
    idxmax_21 = rolling_21.groupby(group_cols).idxmax().reset_index(name='idx_21')

    # Merge idxmax results back with df_daily to get the actual date of max
    # idxmax_7['idx_7'] and idxmax_21['idx_21'] will contain a tuple (seller_sku, seller_id, [context_channel], date)
    # Extract the date from this tuple
    idxmax_7['date_end_max_sold_history_7_days'] = idxmax_7['idx_7'].apply(lambda x: x[-1])
    idxmax_21['date_end_max_sold_history_21_days'] = idxmax_21['idx_21'].apply(lambda x: x[-1])

    # Merge everything together
    df_result = (
        max_7
        .merge(idxmax_7[group_cols + ['date_end_max_sold_history_7_days']], on=group_cols)
        .merge(max_21, on=group_cols)
        .merge(idxmax_21[group_cols + ['date_end_max_sold_history_21_days']], on=group_cols)
    )

    df_result['insertion_date'] = datetime.now()

    return df_result


def create_past_sales(df_sales, num_days):
    logger.info(f"Computing past {num_days} days sales")

    # Set the timezone for São Paulo (UTC-3 or UTC-2 depending on DST)
    sao_paulo = pytz.timezone('America/Sao_Paulo')

    # Get yesterday's date in São Paulo timezone and set to 23:59:59
    end_date = datetime.now(sao_paulo).replace(hour=23, minute=59, second=59) - timedelta(days=1)
    start_date = end_date - timedelta(days=num_days - 1)
    start_date = start_date.replace(hour=0, minute=0, second=0)

    # Convert to UTC for comparison
    start_date_utc = start_date.astimezone(pytz.utc)
    end_date_utc = end_date.astimezone(pytz.utc)

    # Filter the DataFrame for the given date range
    mask = (df_sales.index >= start_date_utc) & (df_sales.index <= end_date_utc)
    df_filtered_sales = df_sales[mask]

    logger.info(f"Filtered {len(df_filtered_sales)} rows for date range: {start_date_utc} to {end_date_utc}")

    # Group by 'seller_sku', 'seller_id', and 'context_channel' (with channel)
    df_past_sales_with_channel = (
        df_filtered_sales
        .groupby(['seller_sku', 'seller_id', 'context_channel'], as_index=False)
        .agg({'quantity': 'sum'})
        .rename(columns={'quantity': f'past_{num_days}_sales'})
    )

    logger.info(f"Computed past {num_days} days sales with channel")

    df_filtered_sales.fillna(0, inplace=True)
    # Group by 'seller_sku' and 'seller_id' (global, without channel)
    df_past_sales_global = (
        df_filtered_sales
        .groupby(['seller_sku', 'seller_id'], as_index=False)
        .agg({'quantity': 'sum'})
        .rename(columns={'quantity': f'past_{num_days}_sales_global'})
    )

    logger.info(f"Computed past {num_days} days sales globally (without channel)")

    logger.info(f"Merged past {num_days} days sales with global sales")

    logger.debug(f"Start date: {start_date}, End date: {end_date}, Rows: {len(df_past_sales_global)}")
    
    return df_past_sales_with_channel, df_past_sales_global