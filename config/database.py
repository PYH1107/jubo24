from pymongo import MongoClient

client = MongoClient("mongodb+srv://maypan1107:{password}@cluster0.qk3cidq.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

db = client.todo_db

collection_name = ("todo_collection")

