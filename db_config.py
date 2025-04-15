from pymongo import MongoClient, IndexModel
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# โหลดค่าจาก .env
load_dotenv()

# ดึงค่า URI และ DB name จาก environment
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('DB_NAME', 'price_data_db')

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

def check_ttl_index():
    """
    ตรวจสอบ TTL index ที่มีอยู่
    """
    try:
        db = get_database()
        if db is None:
            return False

        collection = db['price_data']
        indexes = list(collection.list_indexes())
        print("\nCurrent indexes:")
        for index in indexes:
            print(f"Index: {dict(index)}")
        return True
    except Exception as e:
        print(f"❌ Error checking TTL index: {e}")
        return False

def setup_ttl_index():
    """
    ตั้งค่า TTL index สำหรับลบข้อมูลอัตโนมัติหลัง 1 นาที (สำหรับทดสอบ)
    """
    try:
        db = get_database()
        if db is None:
            return False

        collection = db['price_data']
        
        # Drop all existing indexes except _id
        indexes = list(collection.list_indexes())
        for index in indexes:
            index_info = dict(index)
            if index_info['name'] != '_id_':
                collection.drop_index(index_info['name'])
                print(f"✅ Dropped index: {index_info['name']}")

        # สร้าง TTL index ใหม่
        collection.create_index(
            [("timestamp", 1)],
            expireAfterSeconds=60,
            name="timestamp_ttl_index"
        )
        print("✅ Created new TTL index on 'timestamp' field")
        
        # ตรวจสอบ index ที่สร้าง
        check_ttl_index()
        return True
    except Exception as e:
        print(f"❌ Error setting up TTL index: {e}")
        return False

def store_price_data(data):
    """
    เก็บข้อมูลลงใน MongoDB
    รองรับทั้ง insert_one (dict) และ insert_many (list of dicts)
    """
    try:
        db = get_database()
        if db is None:
            return False

        collection = db['price_data']

        if isinstance(data, list):
            result = collection.insert_many(data)
            print(f"✅ Inserted {len(result.inserted_ids)} documents.")
        else:
            result = collection.insert_one(data)
            print(f"✅ Inserted 1 document with ID: {result.inserted_id}")

        return True
    except Exception as e:
        print(f"❌ Error storing data in MongoDB: {e}")
        return False

# ตั้งค่า TTL index เมื่อ import module
setup_ttl_index()
