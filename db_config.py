from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MongoDB connection settings
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')
DB_NAME = os.getenv('DB_NAME', 'price_data_db')

def get_database():
    """
    Create a database connection and return the database object
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def store_price_data(data):
    """
    Store price data in MongoDB
    """
    try:
        db = get_database()
        if db is None:
            return False
        
        # Create or get the collection
        collection = db['price_data']
        
        # Insert the data
        result = collection.insert_one(data)
        
        if result.inserted_id:
            print(f"✅ Data successfully stored in MongoDB with ID: {result.inserted_id}")
            return True
        return False
    except Exception as e:
        print(f"Error storing data in MongoDB: {e}")
        return False 