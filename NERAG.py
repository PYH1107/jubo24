import os
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import pandas as pd
import jieba
from datetime import datetime, timedelta
import json
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import os

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

# 讀取並打印 health_data 集合中的所有文檔
def read_health_data():
    collection_name = "health_data"
    collection = database[collection_name]
    documents = collection.find()
    print(f"Collection: {collection_name}")
    if collection.count_documents({}) == 0:
        print("No documents found in collection.")
    else:
        for doc in documents:
            print(doc)
            
# 載入中研院 NER 模型
def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese-ner")
    model = AutoModelForTokenClassification.from_pretrained("ckiplab/bert-base-chinese-ner")
    return tokenizer, model

# 預測並提取 NER 實體
def predict_and_extract_entities(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt")
    outputs = model(**inputs)
    labels = torch.argmax(outputs.logits, dim=-1)
    tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
    label_names = model.config.id2label
    results = [(token, label_names[label.item()]) for token, label in zip(tokens, labels[0])]
    return results

# 提取實體函數
def extract_entities(results, entity_type):
    entities = []
    current_entity = []
    for token, label in results:
        if label == f'B-{entity_type}':
            if current_entity:
                entities.append("".join(current_entity))
                current_entity = []
            current_entity.append(token)
        elif label == f'I-{entity_type}' or label == f'E-{entity_type}':
            current_entity.append(token)
        elif current_entity:
            entities.append("".join(current_entity))
            current_entity = []
    if current_entity:
        entities.append("".join(current_entity))
    return entities

# 分詞並提取關鍵詞
def extract_keywords(text, db):
    for keyword in db:
        jieba.add_word(keyword)
    words = jieba.lcut(text)
    keywords = [word for word in words if word in db]
    return keywords

# 將相對日期轉換為具體日期
def relative_date_to_absolute(relative_date):
    today = datetime.today()
    if relative_date == "今日":
        return today.strftime("%Y-%m-%d")
    elif relative_date == "昨日":
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")
    elif relative_date == "前天":
        return (today - timedelta(days=2)).strftime("%Y-%m-%d")
    else:
        return None
    
    # 生成摘要描述
def generate_summary(text_description, api_key):
    url = f'https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={api_key}'
    headers = {'Content-Type': 'application/json'}
    data = {
        "contents": [
            {
                "parts": [{"text": "請為以下數據生成一個自然的摘要描述{" + text_description + "}"}]
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        data = response.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return text
    else:
        return None
    
def prepare_data():
    # 假資料生成，之後這段為真正的 DB
    data = {
        "姓名": ["常小芬"] * 5,
        "日期": ["2024-07-10", "2024-07-11", "2024-07-12", "2024-07-13", "2024-07-14"],
        "心跳(次/分)": [72, 75, 74, 73, 76],
        "血壓(mmHg)": ["120/80", "121/81", "119/79", "122/82", "118/78"],
        "體溫(℃)": [36.5, 36.6, 36.5, 36.7, 36.5],
        "呼吸(次/分)": [18, 19, 18, 17, 19],
        "血氧飽和度(%)": [98, 97, 98, 97, 99]
    }
    return pd.DataFrame(data)

# 去識別化，才能將資料丟入 Public LLM
def process_data(persons, absolute_dates, df):
    if persons and absolute_dates:
        df_filtered = df[(df["姓名"] == persons) & (df["日期"] == absolute_dates)]
        if not df_filtered.empty:
            # 將名字替換成 xxx
            df_filtered_replaced = df_filtered.replace({"姓名": {persons[0]: "xxx"}})
            # 將 DataFrame 轉換成文字描述
            text_description = df_filtered_replaced.to_string(index=False)
            return text_description
    return None

tokenizer, model = load_model_and_tokenizer()
tokenizer

def NERAG(text):
    tokenizer, model = load_model_and_tokenizer()
    results = predict_and_extract_entities(text, tokenizer, model)
    keywords = extract_keywords(text, DB)
    persons = extract_entities(results, "PERSON")
    print("NERAG:" + str(persons[0]))
    dates = extract_entities(results, "DATE")
    absolute_dates = [relative_date_to_absolute(date) for date in dates if relative_date_to_absolute(date)]
    print("NERAG:" + str(absolute_dates[0]))
    if not persons and not dates and not keywords:
        print("No PERSON, DATE, or DB found in the text.")
    else:
        for person in persons:
            print(f"PERSON: {person}")
        for date in dates:
            print(f"DATE: {absolute_dates}")
        for keyword in keywords:
            print(f"DB: {keyword}")
    text_description = process_data(persons[0], absolute_dates[0], df)
    API_KEY = 'AIzaSyCy5y6pFqj5TSce8KDrA26JHdT00NiXrXg'
    summary = generate_summary(text_description, API_KEY)
    if summary:
        summary = summary.replace("xxx", persons[0])
        return summary
    else:
        return "Failed to generate summary."
    
persons = '常小芬'
absolute_dates = '2024-07-14'
df = prepare_data()
df["姓名"]
df["姓名"] == persons
df_filtered = df[(df["姓名"] == persons) & (df["日期"] == absolute_dates)]
df_filtered
text_description = process_data(persons, absolute_dates, df)
API_KEY = 'AIzaSyCy5y6pFqj5TSce8KDrA26JHdT00NiXrXg'
summary = generate_summary(text_description, API_KEY)
summary
text = "常小芬前天的生命跡象"
DB = ["生命跡象", "護理紀錄"]
df = prepare_data()

# 將文字分詞並轉換為模型輸入格式
inputs = tokenizer(text, return_tensors="pt")

# 模型進行預測
outputs = model(**inputs)

# 獲取標籤
labels = torch.argmax(outputs.logits, dim=-1)

# 將標籤轉換回原文字
tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
label_names = model.config.id2label

# 將結果組合回原文字
results = [(token, label_names[label.item()]) for token, label in zip(tokens, labels[0])]

# 提取實體函數
def extract_entities(results, entity_type):
    entities = []
    current_entity = []
    for token, label in results:
        if label == f'B-{entity_type}':
            if current_entity:
                entities.append("".join(current_entity))
                current_entity = []
            current_entity.append(token)
        elif label == f'I-{entity_type}' or label == f'E-{entity_type}':
            current_entity.append(token)
        elif current_entity:
            entities.append("".join(current_entity))
            current_entity = []
    if current_entity:
        entities.append("".join(current_entity))
    return entities
NERAG(text)

import gradio as gr

iface = gr.Interface(
    fn = NERAG,
    inputs = "text",
    outputs = "text",
    title = "NER with Gradio",
    description="請輸入欲查詢的病患，ex: 常小芬昨日的生命跡象")
iface.launch()