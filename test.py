import pymongo
import pandas as pd

# --- กำหนดค่าการเชื่อมต่อ MongoDB ---
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "price_data_db"
COLLECTION_NAME = "price_data"

# --- ชื่อไฟล์ CSV ที่ต้องการบันทึก ---
csv_filename = "output_from_mongodb.csv"

# --- ส่วนหัวของ CSV ---
csv_header = [
    "unitName",
    "headCategory",
    "headCategoryName",
    "commodityCode",
    "commodityNameTH",
    "commodityNameEN",
    "year",
    "month",
    "typeName",
    "priceCur",
    "YearBase",
    "StartYear",
    "StartMonth",
    "EndYear",
    "EndMonth"
]

try:
    # --- เชื่อมต่อกับ MongoDB ---
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # --- ดึงข้อมูลจาก MongoDB ---
    documents = collection.find()

    # --- เก็บข้อมูลทั้งหมดใน list ก่อน ---
    all_data = []

    for doc in documents:
        try:
            item_data = doc.get("item", {})
            request_params = doc.get("request_parameters", {})
            years_data = item_data.get("years", [])

            year_base = request_params.get("YearBase")
            period = request_params.get("Period", {})
            start_year = period.get("StartYear")
            start_month = period.get("StartMonth")
            end_year = period.get("EndYear")
            end_month = period.get("EndMonth")

            for year_info in years_data:
                year = year_info.get("year")
                months_data = year_info.get("months", [])
                for month_info in months_data:
                    row_data = {
                        "unitName": item_data.get("unitName"),
                        "headCategory": item_data.get("headCategory"),
                        "headCategoryName": item_data.get("headCategoryName"),
                        "commodityCode": item_data.get("commodityCode"),
                        "commodityNameTH": item_data.get("commodityNameTH"),
                        "commodityNameEN": item_data.get("commodityNameEN"),
                        "year": year,
                        "month": month_info.get("month"),
                        "typeName": month_info.get("typeName"),
                        "priceCur": month_info.get("priceCur"),
                        "YearBase": year_base,
                        "StartYear": start_year,
                        "StartMonth": start_month,
                        "EndYear": end_year,
                        "EndMonth": end_month
                    }
                    all_data.append(row_data)
        except Exception as e:
            print(f"เกิดข้อผิดพลาดในการประมวลผลเอกสาร: {doc.get('_id')} - {e}")

    # --- แปลงเป็น DataFrame แล้วเซฟเป็น CSV ---
    df = pd.DataFrame(all_data, columns=csv_header)
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

    print(f"✅ ข้อมูลจาก MongoDB ถูกบันทึกเป็นไฟล์ CSV: {csv_filename} เรียบร้อยแล้ว")

except pymongo.errors.ConnectionFailure as e:
    print(f"❌ ไม่สามารถเชื่อมต่อกับ MongoDB ได้: {e}")
except Exception as e:
    print(f"❌ เกิดข้อผิดพลาด: {e}")
finally:
    if 'client' in locals():
        client.close()
