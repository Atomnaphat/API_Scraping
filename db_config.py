from pymongo import MongoClient
from dotenv import load_dotenv
import os

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
