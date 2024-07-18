import os
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import pandas as pd
import jieba
from datetime import datetime as dt, timedelta
import json
import requests
from pymongo import MongoClient
from dotenv import load_dotenv
import gradio as gr
from bson import ObjectId

# 載入環境變數
load_dotenv()

# 從 .env 檔中讀取 MongoDB 連接字串和資料庫名稱
ATLAS_URI = os.getenv("ATLAS_URI")
DB_NAME = os.getenv("DB_NAME")

# 連接到 MongoDB Atlas
print(f"Connecting to MongoDB using URI: {ATLAS_URI}")
client = MongoClient(ATLAS_URI)
database = client[DB_NAME]
print(f"Connected to database: {DB_NAME}")

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

# 自訂 JSON 編碼器
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, dt):
            return o.isoformat()
        return super(JSONEncoder, self).default(o)

# 過濾空欄位
def filter_empty_fields(doc):
    return {k: v for k, v in doc.items() if v}

# 查找並返回符合條件的 patients 集合中的文檔
def read_health_data(last_name, first_name):
    collection_name = "patients"
    collection = database[collection_name]
    query = {"lastName": last_name, "firstName": first_name}
    documents = collection.find(query)
    print(f"Collection: {collection_name}, Query: {query}")
    if collection.count_documents(query) == 0:
        print("No documents found matching the query.")
        return None
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            return doc["_id"], doc["firstName"], doc["lastName"]

# 查找並列印 vitalsigns 集合中的文檔
def read_vital_signs(patient_id, start_date, end_date):
    collection_name = "vitalsigns"
    print(f"Start Date: {start_date}, End Date: {end_date}, Patient ID: {patient_id}")
    collection = database[collection_name]
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": dt.strptime(start_date, "%Y-%m-%d"),
            "$lte": dt.strptime(end_date, "%Y-%m-%d")
        }
    }
    print(f"Query: {query}")
    documents = collection.find(query)
    print(f"Collection: {collection_name}")
    if collection.count_documents(query) == 0:
        print("No documents found in the specified date range.")
        return pd.DataFrame()
    else:
        data = []
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            data.append(filtered_doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
        return pd.DataFrame(data)

# 去識別化，才能將資料丟入 Public LLM
def process_data(first_name, df):
    if not df.empty:
        # 將名字替換成 xxx
        df_replaced = df.replace({"firstName": {first_name: "xxx"}})
        # 將 DataFrame 轉換成文字描述
        text_description = df_replaced.to_string(index=False)
        print(f"Processed text description: {text_description}")
        return text_description
    return None

# 生成摘要描述
def generate_summary(text_description, api_key):
    url = f'https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={api_key}'
    headers = {'Content-Type': 'application/json'}
    data = {
        "contents": [
            {
                "parts": [{"text": f"請為以下資料產生一個摘要描述生命體徵：{text_description}" +""}]
            }
        ]
    }
    print(f"Sending request to API with data: {json.dumps(data, ensure_ascii=False, indent=4)}")
    response = requests.post(url, headers=headers, json=data)
    print(f"API response status code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return text
    else:
        print(f"API response error: {response.text}")
        return None

def NERAG(text, start_year, start_month, start_day, end_year, end_month, end_day):
    tokenizer, model = load_model_and_tokenizer()
    results = predict_and_extract_entities(text, tokenizer, model)
    persons = extract_entities(results, "PERSON")
    
    if not persons:
        print("No PERSON found in the text.")
        return "No PERSON found in the text."
    else:
        for person in persons:
            print(f"PERSON: {person}")

    if persons:
        last_name, first_name = persons[0][0], persons[0][1:]
        patient_id, first_name, last_name = read_health_data(last_name, first_name)
        
        if patient_id:
            start_date = f"{int(start_year):04d}-{int(start_month):02d}-{int(start_day):02d}"
            end_date = f"{int(end_year):04d}-{int(end_month):02d}-{int(end_day):02d}"
            df = read_vital_signs(patient_id, start_date, end_date)
            #print(f"first_name: {first_name}")
            text_description = process_data(first_name, df)
            #print(f"patient: {text_description}")
            if text_description:
                API_KEY = os.getenv('API_KEY')
                summary = generate_summary(text_description, API_KEY)
                if summary:
                    summary = summary.replace("xxx", first_name)
                    return summary
                else:
                    return "Failed to generate summary."
            else:
                return "No data to process."
        else:
            return "Patient not found."
    return "Invalid input."

years = [str(year) for year in range(2015, dt.today().year + 1)]
months = [str(month).zfill(2) for month in range(1, 13)]
days = [str(day).zfill(2) for day in range(1, 32)]

iface = gr.Interface(
    fn=NERAG,
    inputs=[
        gr.Textbox(label="Input Text"),
        gr.Dropdown(choices=years, label="Start Year"),
        gr.Dropdown(choices=months, label="Start Month"),
        gr.Dropdown(choices=days, label="Start Day"),
        gr.Dropdown(choices=years, label="End Year"),
        gr.Dropdown(choices=months, label="End Month"),
        gr.Dropdown(choices=days, label="End Day")
    ],
    outputs="text",
    title="NER with Gradio",
    description="請輸入欲查詢的病患，ex: 常小芬，並選擇日期範圍"
)

iface.launch()
