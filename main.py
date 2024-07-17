#this is the document for fast api

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi import FastAPI

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
collection = db['patients']

# 進行查詢
query = {"lastName": "王", "firstName": {"$regex": "月$"}}
results = collection.find(query)

# 打印結果
for result in results:
    print(result)
