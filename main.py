#this is the document for fast api

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict

from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import re
import jieba
import jieba.analyse
import jieba.posseg as pseg

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

# 2-1: 把我所說的句子傳過來
# 載入中研院 NER 模型
def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese-ner")
    model = AutoModelForTokenClassification.from_pretrained("ckiplab/bert-base-chinese-ner")
    return tokenizer, model

tokenizer, model = load_model_and_tokenizer()

class TextInput(BaseModel):
    text: str



def predict_and_extract_entities(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    outputs = model(**inputs)
    labels = torch.argmax(outputs.logits, dim=-1)
    tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
    label_names = model.config.id2label
    results = [(token, label_names[label.item()]) for token, label in zip(tokens, labels[0])]
    return results

def extract_entities(results, entity_type):
    entities = []
    current_entity = []
    for token, label in results:
        if label.startswith(f'B-{entity_type}'):
            if current_entity:
                entities.append("".join(current_entity))
                current_entity = []
            current_entity.append(token)
        elif label.startswith(f'I-{entity_type}') or label.startswith(f'E-{entity_type}'):
            if token.startswith("##"):
                current_entity[-1] += token[2:]
            else:
                current_entity.append(token)
        elif current_entity:
            entities.append("".join(current_entity))
            current_entity = []
    if current_entity:
        entities.append("".join(current_entity))
    return entities



# 有 keyword DB
def extract_keywords(text, db):
    for keyword in db:
        jieba.add_word(keyword)
    words = jieba.lcut(text)
    keywords = [word for word in words if word in db]
    return keywords


def extract_date(text):
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    dates = re.findall(date_pattern, text)
    return dates

@app.post("/extract_entities")
async def api_extract_entities(input: TextInput):
    if not input.text:
        raise HTTPException(status_code=400, detail="Text input is required")

    results = predict_and_extract_entities(input.text, tokenizer, model)
    person_names = extract_entities(results, 'PER')
    dates = extract_date(input.text)
    # 沒有 keyword db
    # keywords = extract_keywords(input.text)

    # 假設我們有一個關鍵詞數據庫
    keyword_db = set(["關鍵詞1", "關鍵詞2", "關鍵詞3"])  # 這裡需要替換為實際的關鍵詞數據庫
    keywords = extract_keywords(input.text, keyword_db)

    return {
        "dates": dates,
        "person_names": person_names,
        "keywords": keywords
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


'''
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
'''