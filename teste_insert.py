import json
from types import SimpleNamespace
from src.cloud_functions._1_fetch_data._1_2_fetch_costs.main import fetch_costs_data
from src.cloud_functions._1_fetch_data._1_5_fetch_prices.main import fetch_prices_data
from src.cloud_functions._1_fetch_data._1_1_fetch_details.main import fetch_details_data
from src.cloud_functions._1_fetch_data._1_10_fetch_variations.main import fetch_variations_data
from src.cloud_functions._1_fetch_data._1_0_fetch_items.main import fetch_items_data
from src.cloud_functions._1_fetch_data._1_2_fetch_costs.main import fetch_costs_data
from src.cloud_functions._2_insert_bq._2_1_insert_bq_details.main import insert_bq_details
from src.cloud_functions._2_insert_bq._2_5_insert_bq_prices.main import insert_bq_prices
from src.cloud_functions._2_insert_bq._2_2_insert_bq_costs.main import insert_bq_costs
from src.cloud_functions._1_fetch_data._1_11_fetch_ranking_catalog.main import fetch_ranking_catalog
from src.cloud_functions._2_insert_bq._2_10_insert_bq_competitors_catalog.main import insert_bq_competitors_catalog

from src.cloud_functions._1_fetch_data._1_12_fetch_free_shipping_status.main import fetch_free_shipping_status
from src.cloud_functions._2_insert_bq._2_11_insert_bq_free_shipping_status.main import insert_bq_free_shipping_status

from src.cloud_functions._1_fetch_data._1_7_fetch_competitors_price.main import fetch_competitors_prices_data
from src.cloud_functions._2_insert_bq._2_7_insert_bq_competitors_price.main import insert_bq_competitors_prices
from src.cloud_functions._4_models.predicted_sales.main import get_max_sales_history
from src.cloud_functions._1_fetch_data._0_2_fetch_history_visits.main import fetch_visits_data
from src.cloud_functions._2_insert_bq._2_12_insert_bq_visits.main import insert_bq_visits

from src.cloud_functions._5_trigger_workflow.main import triggers_workflow

from src.cloud_functions._1_fetch_data._1_13_fetch_items_promotions.main import fetch_promotions_data

from src.cloud_functions._1_fetch_data._1_13_fetch_items_promotions.main import fetch_promotions_data
from src.cloud_functions._2_insert_bq._2_13_insert_bq_promotions.main import insert_bq_promotions

from src.cloud_functions._6_get_sellers_information.main import main_fetch_sellers_information

from src.cloud_functions._1_fetch_data._1_7_fetch_competitors_price.main import fetch_competitors_prices_data
from src.cloud_functions._1_fetch_data._1_9_fetch_competitors_details.main import fetch_competitors_details_data

from src.cloud_functions._2_insert_bq._2_9_insert_bq_competitors_details.main import insert_bq_competitors_details
from src.cloud_functions._2_insert_bq._2_7_insert_bq_competitors_price.main import insert_bq_competitors_prices

class MockRequest:    

    def __init__(self, json_data):
        self._json_data = json_data

    def get_json(self):
        return self._json_data

#triggers_workflow({})
# Your test JSON data
test_data = {
 "access_token": None,
 "client_id": "4959083987776428",
 "client_secret": "Hw9wWSydd8PMvMEJewWoMvKGYMAWyKEw",
 "seller_id": 189643563,
 "store_name": "hubsmarthome"
}

mock_request = MockRequest(test_data)

result = fetch_competitors_prices_data(mock_request)
result = insert_bq_competitors_prices(mock_request)

result = fetch_competitors_details_data(mock_request)
result = insert_bq_competitors_details(mock_request)

# generate_competitors_main({})

# # test_data = {
#    "access_token": "APP_USR-2951712600123976-102309-17e9ae2af96480acf2d3bb33d6de6d71-569119547",
#    "client_id": "2951712600123976",
#    "client_secret": "QprAIl8ydXzcxFVHjnIHT6fUQ8KpzADV",
#    "seller_id": 569119547,
#    "store_name": "gw shop"
#  }

# mock_request = MockRequest(test_data)

# # # Call your function with the mock request
# result = fetch_costs_data(mock_request)

##result = fetch_visits_data(mock_request)
# # result = insert_bq_visits(mock_request)

# result = insert_bq_details(mock_request)
# result =insert_bq_prices(mock_request)

# # Print the result
# print(result)

# from flask import Flask
# from src.cloud_functions.import_data.store_import_data.main import store_import_data

# # Create a Flask app
# app = Flask(__name__)

# # Mock request class to simulate Flask's request object
# class MockRequest:
#     def __init__(self, json_data):
#         self._json_data = json_data

#     def get_json(self):
#         return self._json_data

# test_data = {
#     "file_type": "competitors",
#     "seller_id": "189643563",
#     "store_identifier": "hubsmarthome"
# }

# # Create the mock request
# mock_request = MockRequest(test_data)

# # Use the app context to test
# with app.app_context():
#     result = store_import_data(mock_request)

# # Print the result
# print(result)


# from src.cloud_functions._4_models.predicted_sales.main import get_max_sales_history

# request = {}
# get_max_sales_history(request)