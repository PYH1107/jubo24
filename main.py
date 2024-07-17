#this is the document for fast api

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

password = os.getenv('MONGODB_PASSWORD')
uri = f"mongodb+srv://maypan1107:{password}@cluster0.qk3cidq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)