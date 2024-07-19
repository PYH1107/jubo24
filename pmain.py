# 潘允蕙拿來實驗的半成品

import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import re
import jieba
import jieba.analyse
import jieba.posseg as pseg
import requests
import pandas as pd

# 第一部分: 環境
app = FastAPI()

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("API_KEY")  # 從環境變量中讀取 API_KEY
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

vitalsigns_collection = db["vitalsigns"]


# 載入中研院 NER 模型
def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese-ner")
    model = AutoModelForTokenClassification.from_pretrained("ckiplab/bert-base-chinese-ner")
    return tokenizer, model

tokenizer, model = load_model_and_tokenizer()

class TextInput(BaseModel):
    text: str


# 第二部分: 提取

# 2-1 labels
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


# 2-2: 提取姓名，並區別姓氏與名字
first_name=""
last_name=""
def extract_name_parts(full_name):
    # 假設中文名字格式：姓氏 + 名字
    if len(full_name) > 1:
        last_name = full_name[0]
        first_name = full_name[1:]
        print("last_name=" + last_name)
        print("first_name=" + first_name)
        return {"firstName": first_name, "lastName": last_name}
    else:
        return {"firstName": full_name, "lastName": ""}


# 有 keyword DB
DB = ["生命跡象", "護理紀錄"]

def extract_keywords(text, db):
    for keyword in db:
        jieba.add_word(keyword)
    words = jieba.lcut(text)
    keywords = [word for word in words if word in db]
    return keywords


# 2-2 提取日期
def extract_date(text):
    date_patterns = [
        r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',  # MM-DD-YYYY or MM/DD/YYYY
        r'\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYYY-MM-DD or YYYY/MM/DD
        r'\b(\d{2,3})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYY-MM-DD or YYY/MM/DD (民國年)
        r'\b(\d{4})年(\d{1,2})月(\d{1,2})日\b',  # YYYY年MM月DD日
        r'\b(\d{1,2})月(\d{1,2})日(\d{4})年\b',  # MM月DD日YYYY年
        r'\b民國(\d{1,3})年(\d{1,2})月(\d{1,2})日\b',  # 民國YYY年MM月DD日
    ]

    dates = []
    for pattern in date_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            try:
                groups = match.groups()
                if len(groups) == 3:
                    if '年' in pattern or '月' in pattern or '日' in pattern:
                        if '民國' in pattern:
                            year = int(groups[0]) + 1911
                        else:
                            year = int(groups[0])
                        month = int(groups[1])
                        day = int(groups[2])
                    elif len(groups[0]) == 4:  # YYYY-MM-DD
                        year, month, day = map(int, groups)
                    elif len(groups[2]) == 4:  # DD-MM-YYYY
                        month, day, year = map(int, groups)
                    else:  # YYY-MM-DD (民國年)
                        year = int(groups[0]) + 1911
                        month, day = map(int, groups[1:])

                    if 1 <= month <= 12 and 1 <= day <= 31:
                        if year < 1911:  # 處理可能的民國年份
                            year += 1911
                        parsed_date = datetime(year, month, day)
                        formatted_date = parsed_date.strftime('%Y-%m-%d')
                        if formatted_date not in dates:
                            dates.append(formatted_date)
            except ValueError:
                # 如果日期無效，跳過
                continue

    return sorted(dates)  # 按日期排序

# 3: 連接 DB

# 3-1 从 patient 集合中提取病人 id
def get_patient_id(firstname, lastname):
    patients_collection = db["patients"]

    query = {
        "firstName": firstname,
        "lastName": lastname
    }

    patient = patients_collection.find_one(query)

    if patient:
        return patient.get('_id')  # 返回患者的 MongoDB _id
    else:
        return None

# 3-2 通过 id 查找 vitalsigns
def get_vitalsigns(patient_id):
    vitalsign_collection = db["vitalsign"]

    query = {
        "patientId": patient_id
    }

    cursor = vitalsign_collection.find(query)
    df = pd.DataFrame(list(cursor))

    if df.empty:
        return df

    if '_id' in df.columns:
        df['id'] = df['_id'].astype(str)
        df = df.drop('_id', axis=1)

    return df

# 3-3 整合以上两个函数
def prepare_patient_data(firstname, lastname):
    patient_id = get_patient_id(firstname, lastname)
    if patient_id:
        return get_vitalsigns(patient_id)
    else:
        return pd.DataFrame()  # 如果没找到病人，返回空 DataFrame















def generate_summary(text_description):
    url = f'https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={API_KEY}'
    headers = {'Content-Type': 'application/json'}
    data = {
        "contents": [
            {
                "parts": [{"text": f"請為以下數據生成一個自然的摘要描述{{{text_description}}}"}]
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["candidates"][0]["content"]["parts"][0]["text"]
    return None


'''
@app.post("/extract_entities_dif")
async def api_extract_entities(input: TextInput):
    if not input.text:
        raise HTTPException(status_code=400, detail="Text input is required")


    results = predict_and_extract_entities(input.text, tokenizer, model)
    person_names = extract_entities(results, 'PER')
    dates = extract_date(input.text)


    keywords = extract_keywords(input.text, DB)  # 這裡要傳入 DB


    name_parts = [extract_name_parts(name) for name in person_names]
    print("last_name=" + last_name)
    print("first_name=" + first_name)


    return {
        "dates": dates,
        "person_names": name_parts,
        "keywords": keywords
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    '''
'''
class PatientRequest(BaseModel):
    firstName: str
    lastName: str

@app.post("/patient-vitalsigns")
async def get_patient_vitalsigns(patient: PatientRequest):
    data = prepare_patient_data(patient.firstName, patient.lastName)
    if data.empty:
        raise HTTPException(status_code=404, detail="Patient not found or no vitalsigns data available")
    return data.to_dict(orient="records")
'''