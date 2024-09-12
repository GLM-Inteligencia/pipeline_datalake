import json
from types import SimpleNamespace
from src.cloud_functions.fetch_data.fetch_costs.main import fetch_costs_data


# Mock request class to simulate Flask's request object
class MockRequest:
    def __init__(self, json_data):
        self._json_data = json_data

    def get_json(self):
        return self._json_data

# Your test JSON data
test_data = {
    "access_token": None,
    "client_id": "4959083987776428",
    "client_secret": "Hw9wWSydd8PMvMEJewWoMvKGYMAWyKEw",
    "seller_id": 189643563,
    "store_name": "hubsmarthome"
}

# Create the mock request
mock_request = MockRequest(test_data)

# Call your function with the mock request
# result = main_fetch(mock_request)
result = fetch_costs_data(mock_request)

# Print the result
print(result)


