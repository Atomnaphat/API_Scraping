import requests
from datetime import datetime, timedelta
import db_config  # Import the entire module to ensure TTL index is initialized
from db_config import store_price_data
import time
from datetime import timezone

def get_thailand_time():
    """
    สร้างเวลาปัจจุบันในโซนเวลาของประเทศไทย (UTC+7)
    """
    utc_now = datetime.utcnow()  # Use UTC time for MongoDB
    return utc_now

def fetch_and_store_data():
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
        timestamp_now = get_thailand_time()  # ใช้เวลา UTC สำหรับ MongoDB
        documents = [{
            "item": item,
            "timestamp": timestamp_now,  # Store as UTC
            "request_parameters": data
        } for item in response_data]

        # เก็บข้อมูลลง MongoDB
        if store_price_data(documents):
            print(f"✅ Stored {len(documents)} documents in MongoDB")
            # แสดงเวลาที่จะลบในเวลาประเทศไทย
            deletion_time_utc = timestamp_now + timedelta(days=90)  # 3 months = 90 days
            deletion_time_th = deletion_time_utc + timedelta(hours=7)
            print(f"⏰ Documents will be automatically deleted at: {deletion_time_th.strftime('%Y-%m-%d %H:%M:%S')} (Thailand time)")
            print(f"📌 Current UTC time: {timestamp_now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
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

if __name__ == "__main__":
    print("🚀 Starting data fetch...")
    fetch_and_store_data()
    print("✅ Program completed")
