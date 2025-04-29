from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import os

# โหลดค่าจาก .env
load_dotenv()

# ดึงค่า URI และ DB name จาก environment
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('DB_NAME', 'TPSO_logs')

def get_database():
    """
    สร้างและส่งคืน database object จาก MongoDB
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return None

def store_price_data(data, use_timestamp=True):
    """
    เก็บข้อมูลลงใน MongoDB
    รองรับทั้ง insert_one (dict) และ insert_many (list of dicts)
    จะสร้าง collection แยกตามวันที่หรือวัน+เวลาแล้วแต่พารามิเตอร์ use_timestamp
    """
    try:
        db = get_database()
        if db is None:
            return False

        # เวลาประเทศไทย (UTC+7)
        thailand_time = datetime.now(timezone(timedelta(hours=7)))

        if use_timestamp:
            collection_name = thailand_time.strftime("TPSO_Data_%d-%m-%Y_%H-%M")
        else:
            collection_name = thailand_time.strftime("TPSO_Data_%d-%m-%Y")

        collection = db[collection_name]

        # เช็คประเภทข้อมูล
        if isinstance(data, list):
            if not data:
                print("⚠️ No data to insert.")
                return False
            result = collection.insert_many(data)
            print(f"✅ Inserted {len(result.inserted_ids)} documents into '{collection_name}'")
        else:
            result = collection.insert_one(data)
            print(f"✅ Inserted 1 document with ID: {result.inserted_id} into '{collection_name}'")

        return True
    except Exception as e:
        print(f"❌ Error storing data in MongoDB: {e}")
        return False
