# import json
# from types import SimpleNamespace
# from src.cloud_functions._1_fetch_data._1_2_fetch_costs.main import fetch_costs_data
# from src.cloud_functions._2_insert_bq._2_2_insert_bq_costs.main import insert_bq_costs
# # Mock request class to simulate Flask's request object
# class MockRequest:
#     def __init__(self, json_data):
#         self._json_data = json_data

#     def get_json(self):
#         return self._json_data

# # Your test JSON data
# test_data = {
#   "access_token": None,
#   "client_id": "4959083987776428",
#   "client_secret": "Hw9wWSydd8PMvMEJewWoMvKGYMAWyKEw",
#   "seller_id": 189643563,
#   "store_name": "hubsmarthome"
# }

# # test_data = {
# #     "file_type": "competitors",
# #     "seller_id": "189643563",
# #     "store_name": "hubsmarthome"
# # }
# # # Create the mock request
# mock_request = MockRequest(test_data)

# # Call your function with the mock request
# # result = fetch_historic_orders(mock_request)
# result = fetch_costs_data(mock_request)
# result =insert_bq_costs(mock_request)

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


from src.cloud_functions._4_models.predicted_sales.main import get_max_sales_history

request = {}
get_max_sales_history(request)
