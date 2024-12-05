
# Service account
PATH_SERVICE_ACCOUNT = "C:/Users/User/Documents/papa preco/service account/service_account_datalakev2.json"
# PATH_SERVICE_ACCOUNT = "D:/bacar/Savvi/GLM/Desenvolvimento/service_account/service_account_datalakev2.json"

# Tables Bigquery
TABLE_MANAGEMENT = "datalake-v2-424516.datalake_v2.datalake_management"
TABLE_CATALOG = "datalake-v2-424516.datalake_v2.items_catalog"
TABLE_COSTS = "datalake-v2-424516.datalake_v2.items_costs"
TABLE_DETAILS = "datalake-v2-424516.datalake_v2.items_details"
TABLE_FULLFILMENT = "datalake-v2-424516.datalake_v2.items_fullfilment"
TABLE_PRICES = "datalake-v2-424516.datalake_v2.items_prices"
TABLE_COMPETITORS_PRICES = "datalake-v2-424516.datalake_v2.items_competitors_prices"
TABLE_COMPETITORS_DETAILS = "datalake-v2-424516.datalake_v2.items_competitors_details"
TABLE_SHIPPING_COSTS = "datalake-v2-424516.datalake_v2.items_shipping_cost"
TABLE_VISITS = "datalake-v2-424516.datalake_v2.items_visits"
TABLE_ORDERS = "datalake-v2-424516.datalake_v2.orders"
TABLE_PREDICTED_SALES = "datalake-v2-424516.predictions.sales"
TABLE_INPUT_COMPETITORS = "datalake-v2-424516.datalake_v2.competitors_suggestions_v2"
TABLE_VARIATIONS = "datalake-v2-424516.datalake_v2.items_variations"
TABLE_CATALOG_COMPETITORS = "datalake-v2-424516.datalake_v2.items_competitors_catalog"
TABLE_FREE_SHIPPING_STATUS = "datalake-v2-424516.datalake_v2.items_free_shipping_status"
TABLE_SELLER_INFORMATION = 'datalake-v2-424516.datalake_v2.update_sellers_competitors_details'
TABLE_ITEM_PROMOTION = "datalake-v2-424516.datalake_v2.items_promotions"
TABLE_EXTERNAL_ORDERS = "datalake-v2-424516.datalake_v2.external_orders"


# Bucket name
BUCKET_STORES = "glm-store"

# Blob names
BLOB_PRICES = lambda store_name : f'{store_name}/meli/api_response/item_price/'
BLOB_PRICES_MSHOPS = lambda store_name : f'{store_name}/meli/api_response/item_price_mshops/'

BLOB_COMPETITORS_PRICES = lambda store_name : f'{store_name}/meli/api_response/item_competitors_price/'
BLOB_COMPETITORS_DETAILS = lambda store_name : f'{store_name}/meli/api_response/item_competitors_details/'
BLOB_COMPETITORS_CATALOG = lambda store_name : f'{store_name}/meli/api_response/item_competitors_catalog/'
BLOB_CATALOG = lambda store_name : f'{store_name}/meli/api_response/catelog_details/' 
BLOB_FULLFILMENT= lambda store_name : f'{store_name}/meli/api_response/item_fullfilment/'
BLOB_COSTS = lambda store_name : f'{store_name}/meli/api_response/item_cost/'
BLOB_SHIPPING_COST = lambda store_name : f'{store_name}/meli/api_response/item_shipping/'
BLOB_ITEMS = lambda store_name : f'{store_name}/meli/api_response/items/'
BLOB_ITEMS_DETAILS = lambda store_name : f'{store_name}/meli/api_response/item_detail/'
BLOB_VARIATIONS = lambda store_name : f'{store_name}/meli/api_response/variation_detail/'
BLOB_ORDERS = lambda store_name : f'{store_name}/meli/api_response/orders/'
BLOB_FREE_SHIPPING_STATUS = lambda store_name : f'{store_name}/meli/api_response/item_free_shipping_status/'
BLOB_VISITS = lambda store_name : f'{store_name}/meli/api_response/item_visits/'
BLOB_PROMOTIONS = lambda store_name : f'{store_name}/meli/api_response/items_promotions/'
BLOB_PROMOTIONS_MSHOPS = lambda store_name : f'{store_name}/meli/api_response/items_promotions_mshops/'

# URLs API
URL_PRICE_MARKETPLACE = lambda item_id: f"https://api.mercadolibre.com/items/{item_id}/sale_price?context=channel_marketplace"
URL_PRICE_MSHOPS = lambda item_id: f"https://api.mercadolibre.com/items/{item_id}/sale_price?context=channel_mshops"
URL_CATALOG = lambda item_id: f'https://api.mercadolibre.com/items/{item_id}/price_to_win?version=v2'
URL_FULLFILMENT = lambda inventory_id : f"https://api.mercadolibre.com/inventories/{inventory_id}/stock/fulfillment"
URL_COST = 'https://api.mercadolibre.com/sites/MLB/listing_prices'
URL_SHIPPING_COST = lambda item_id, seller_id : f'https://api.mercadolibre.com/users/{seller_id}/shipping_options/free?item_id={item_id}'
URL_ITEMS = lambda seller_id : f'https://api.mercadolibre.com/users/{seller_id}/items/search'
URL_ITEM_DETAIL = lambda item_id: f'https://api.mercadolibre.com/items/{item_id}'
URL_VARIATIONS = lambda item_id, variation_id: f'https://api.mercadolibre.com/items/{item_id}/variations/{variation_id}'
URL_ORDERS = lambda seller_id : f"https://api.mercadolibre.com/orders/search?seller={seller_id}"
URL_COMPETITORS_CATALOG = lambda catalog_item_id : f"https://api.mercadolibre.com/products/{catalog_item_id}/items"
URL_FREE_SHIPPING_STATUS = lambda item_id : f"https://api.mercadolibre.com/items/{item_id}/shipping"
URL_VISITS = lambda item_id, n : f'https://api.mercadolibre.com/items/{item_id}/visits/time_window?last={n}&unit=day'

URL_PROMOTIONS_MARKETPLACE = lambda item_id: f"https://api.mercadolibre.com/seller-promotions/items/{item_id}?app_version=v2"
URL_PROMOTIONS_MSHOPS = lambda item_id: f"https://api.mercadolibre.com/seller-promotions/items/{item_id}?channel=mshops&limit=100&offset=0"

# GCP INFO
FIRESTORE_COLLECTION_USERS = 'users_credentials'
PROJECT_ID_FIREBASE = 'datalake-meli-dev'
PROJECT_ID_WORKFLOW = "datalake-v2-424516"
LOCATION = "southamerica-east1"
WORKFLOW_NAME = "workflow-functions-datalakev2"

