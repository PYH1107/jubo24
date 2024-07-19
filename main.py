from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from datetime import datetime
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import re
import os
import jieba
import jieba.analyse
import jieba.posseg as pseg

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()
first_name=""
last_name=""
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

def extract_date(text):
    date_patterns = [
        r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',  # DD-MM-YYYY or DD/MM/YYYY
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
                        day, month, year = map(int, groups)
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
