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

#2.
class Name(BaseModel):
    lastName: str
    firstName: str


@app.post("/search_patients_vitals", response_model=List[Dict])
async def search_patients_vitals(name: Name):
    query = {"lastName": name.lastName, "firstName": name.firstName}
    patient_ids = patients_collection.find(query, {"_id": 1})
    results = []
    for patient in patient_ids:
        patient_data = {"patient_id": str(patient["_id"])}
        vitals = list(vitalsigns_collection.find({"patient_id": patient["_id"]}))
        patient_data["vitals"] = vitals
        results.append(patient_data)
    if not results:
        raise HTTPException(status_code=404, detail="No patient found")
    return results