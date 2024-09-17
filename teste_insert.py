# import json
# from types import SimpleNamespace
# from src.cloud_functions.fetch_data.fetch_history_orders.main import fetch_historic_orders
# from src.cloud_functions.insert_bq.insert_bq_orders.main import insert_bq_orders
# from src.cloud_functions.import_data.store_import_data.main import store_import_data

# # Mock request class to simulate Flask's request object
# class MockRequest:
#     def __init__(self, json_data):
#         self._json_data = json_data

#     def get_json(self):
#         return self._json_data

# # Your test JSON data
# # test_data = {
# #   "access_token": None,
# #   "client_id": "4959083987776428",
# #   "client_secret": "Hw9wWSydd8PMvMEJewWoMvKGYMAWyKEw",
# #   "seller_id": 189643563,
# #   "store_name": "hubsmarthome"
# # }

# test_data = {
#     "file_type": "competitors",
#     "seller_id": "189643563",
#     "store_name": "hubsmarthome"
# }
# # Create the mock request
# mock_request = MockRequest(test_data)

# # Call your function with the mock request
# # result = fetch_historic_orders(mock_request)
# result = store_import_data(mock_request)

# # Print the result
# print(result)




from flask import Flask
from src.cloud_functions.import_data.store_import_data.main import store_import_data

# Create a Flask app
app = Flask(__name__)

# Mock request class to simulate Flask's request object
class MockRequest:
    def __init__(self, json_data):
        self._json_data = json_data

    def get_json(self):
        return self._json_data

test_data = {
    "file_type": "sku_data",
    "seller_id": "189643563",
    "store_identifier": "hubsmarthome"
}

# Create the mock request
mock_request = MockRequest(test_data)

# Use the app context to test
with app.app_context():
    result = store_import_data(mock_request)

# Print the result
print(result)

