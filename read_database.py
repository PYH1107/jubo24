from pymongo import MongoClient
from dotenv import load_dotenv
import os
import json
from bson import ObjectId
import datetime

# 加載環境變量
load_dotenv()

# 從 .env 文件中讀取 MongoDB 連接字符串和資料庫名稱
ATLAS_URI = os.getenv("ATLAS_URI")
DB_NAME = os.getenv("DB_NAME")

# 連接到 MongoDB Atlas
print(f"Connecting to MongoDB using URI: {ATLAS_URI}")
client = MongoClient(ATLAS_URI)
database = client[DB_NAME]
print(f"Connected to database: {DB_NAME}")

# 自訂 JSON 編碼器
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super(JSONEncoder, self).default(o)

# 過濾空欄位
def filter_empty_fields(doc):
    return {k: v for k, v in doc.items() if v}

# 查找並返回符合條件的 patients 集合中的文檔
def read_health_data():
    collection_name = "patients"
    collection = database[collection_name]
    query = {"lastName": "董", "firstName": {"$regex": ".*萱.*"}}
    documents = collection.find(query)
    print(f"Collection: {collection_name}")
    if collection.count_documents(query) == 0:
        print("No documents found matching the query.")
        return None
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            return doc["_id"]

# 查找並打印 vitalsigns 集合中的文檔
def read_vital_signs(patient_id, start_date, end_date):
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": datetime.datetime.strptime(start_date, "%Y-%m-%d"),
            "$lte": datetime.datetime.strptime(end_date, "%Y-%m-%d")
        }
    }
    projection = {"PR": 1, "RR": 1, "SYS": 1, "TP": 1, "DIA": 1, "SPO2": 1, "PAIN": 1, "createdDate": 1, "_id": 0}  # 投影指定欄位
    documents = database["vitalsigns"].find(query, projection)
    if database["vitalsigns"].count_documents(query) == 0:
        print("No documents found in the specified date range.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))

def read_nursingnotedetails(patient_id, start_date, end_date):
    collection_name = "nursingnotes"
    collection = database[collection_name]
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": datetime.datetime.strptime(start_date, "%Y-%m-%d"),
            "$lte": datetime.datetime.strptime(end_date, "%Y-%m-%d")
        }
    }
    documents = collection.find(query)
    print(f"Collection: {collection_name}")
    if collection.count_documents(query) == 0:
        print("No documents found in the specified date range.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))

if __name__ == "__main__":
    patient_id = read_health_data()
    if patient_id:
        # 指定查詢的日期範圍
        start_date = "2019-06-14"
        end_date = "2019-06-18"
        read_vital_signs(patient_id, start_date, end_date)
        read_nursingnotedetails(patient_id, start_date, end_date)
