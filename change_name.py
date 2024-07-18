#this is the document for fast api

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict


# Load environment variables
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

password = os.getenv('MONGODB_PASSWORD')
uri = f"mongodb+srv://ai-nerag:{password}@ai-nerag.iiltl.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)


# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client['release']
patients_collection = db["patients"]
vitalsigns_collection = db["vitalsigns"]


# 1. firstname: 把 "常*芬" 改成 "常小芬"
# 計數器
update_count = 0

try:
    # 遍歷並更新 firstname
    for document in patients_collection.find():
        original_firstname = document.get('firstName', '')
        if '*' in original_firstname:
            updated_firstname = original_firstname.replace("*", "小")
            result = patients_collection.update_one({'_id': document['_id']}, {'$set': {'firstname': updated_firstname}})
            if result.modified_count > 0:
                update_count += 1
                print(f"更新了文檔: {document['_id']}, 從 '{original_firstname}' 到 '{updated_firstname}'")
        else:
            print(f"無需更新文檔: {document['_id']}, firstname: '{original_firstname}'")

    print(f"更新完成。總共更新了 {update_count} 個文檔。")

except Exception as e:
    print(f"發生錯誤: {e}")

finally:
    client.close()
    print("MongoDB 連接已關閉。")

# 2. 發現四個字的名字會有 lastName 也有 "*" 的狀況出現，所以把這邊也改成"小"

# 計數器
update_count = 0

# 檢查集合是否存在
if 'patients' in db.list_collection_names():
    print("patients 集合存在")
    # 檢查集合中的文檔數量
    doc_count = patients_collection.count_documents({})
    print(f"patients 集合中有 {doc_count} 個文檔")
else:
    print("patients 集合不存在")
    exit(1)  # 如果集合不存在，退出程序

try:
    # 遍歷並更新 firstname
    for document in patients_collection.find():
        original_lastName = document.get('lastName', '')
        if '*' in original_lastName:
            updated_lastName = original_lastName.replace("*", "小")
            result = patients_collection.update_one({'_id': document['_id']}, {'$set': {'lastName': updated_lastName}})
            if result.modified_count > 0:
                update_count += 1
                print(f"更新了文檔: {document['_id']}, 從 '{original_lastName}' 到 '{updated_lastName}'")
        else:
            print(f"無需更新文檔: {document['_id']}, lastName: '{original_lastName}'")

    print(f"更新完成。總共更新了 {update_count} 個文檔。")

except Exception as e:
    print(f"發生錯誤: {e}")

finally:
    client.close()
    print("MongoDB 連接已關閉。")


