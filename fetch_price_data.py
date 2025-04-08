import requests
from datetime import datetime
from db_config import store_price_data

url = "https://index-api.tpso.go.th/api/cmip/filter"

# พารามิเตอร์สำหรับ POST request
data = {
    "YearBase": 2558,
    "Categories": [],
    "HeadCategories": [
        "checkAll", "01", "02", "03", "04", "05", "06", "07", "08", "09",
        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"
    ],
    "Period": {
        "StartYear": "2565",
        "StartMonth": 1,
        "EndYear": "2565",
        "EndMonth": 12
    },
    "Search": "",
    "TimeOption": True,
    "Types": ["14"]
}

# เรียก API
response = requests.post(url, json=data)

if response.status_code == 200:
    print("✅ Success! Fetching data...")
    response_data = response.json()

    # เตรียมข้อมูลเป็นรายการแยก ๆ
    timestamp_now = datetime.now()
    documents = [{
        "item": item,
        "timestamp": timestamp_now,
        "request_parameters": data
    } for item in response_data]

    # เก็บข้อมูลลง MongoDB
    if store_price_data(documents):
        print(f"✅ Stored {len(documents)} documents in MongoDB")
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
