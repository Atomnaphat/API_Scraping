import requests
from db_config import store_price_data
from datetime import datetime

url = "https://index-api.tpso.go.th/api/cmip/filter"

data = {
  "YearBase": 2558,
  "Categories": [],
  "HeadCategories": [
    "checkAll",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21"
  ],
  "Period": {
    "StartYear": "2565",
    "StartMonth": 1,
    "EndYear": "2565",
    "EndMonth": 12
  },
  "Search": "",
  "TimeOption": True,
  "Types": [
    "14"
  ]
}

response = requests.post(url, json=data)
print(response)

if response.status_code == 200:
    print("✅ Success! Response Data:")
    response_data = response.json()
    print(response_data)
    
    # Prepare data for MongoDB storage
    data_to_store = {
        "api_response": response_data,
        "timestamp": datetime.now(),
        "request_parameters": data
    }
    
    # Store in MongoDB
    if store_price_data(data_to_store):
        print("✅ Data successfully stored in MongoDB")
    else:
        print("❌ Failed to store data in MongoDB")
else:
    print(f"❌ Request failed with status {response.status_code}")
    print(f"Response Text: {response.text}")
    try:
        error_json = response.json()
        print(f"Error Details: {error_json}")
    except ValueError:
        print("Could not parse error response as JSON.")