import json
from types import SimpleNamespace
from src.cloud_functions.fetch_data.fetch_details.main import fetch_details_data 


# Mock request class to simulate Flask's request object
class MockRequest:
    def __init__(self, json_data):
        self._json_data = json_data

    def get_json(self):
        return self._json_data

# Your test JSON data
test_data = {
  "access_token": "APP_USR-2951712600123976-091209-e4cbcf8382b99c6a63784766757e8fb6-569119547",
  "client_id": "2951712600123976",
  "client_secret": "QprAIl8ydXzcxFVHjnIHT6fUQ8KpzADV",
  "seller_id": 569119547,
  "store_name": "gw shop"
}

# Create the mock request
mock_request = MockRequest(test_data)

# Call your function with the mock request
# result = main_fetch(mock_request)
result = fetch_details_data(mock_request)

# Print the result
print(result)


