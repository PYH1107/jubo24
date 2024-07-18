# 重新整理老師的程式
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import pandas as pd
import jieba
from datetime import datetime, timedelta
import requests
import gradio as gr

# 環境設置
load_dotenv()
API_KEY = os.getenv("API_KEY")  # 從環境變量中讀取 API_KEY

# 模型加載
def load_model_and_tokenizer():
    tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese-ner")
    model = AutoModelForTokenClassification.from_pretrained("ckiplab/bert-base-chinese-ner")
    return tokenizer, model

tokenizer, model = load_model_and_tokenizer()

# 數據處理函數
def predict_and_extract_entities(text, tokenizer, model):
    inputs = tokenizer(text, return_tensors="pt")
    outputs = model(**inputs)
    labels = torch.argmax(outputs.logits, dim=-1)
    tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
    label_names = model.config.id2label
    return [(token, label_names[label.item()]) for token, label in zip(tokens, labels[0])]

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

def extract_keywords(text, db):
    for keyword in db:
        jieba.add_word(keyword)
    words = jieba.lcut(text)
    return [word for word in words if word in db]

def relative_date_to_absolute(relative_date):
    today = datetime.today()
    if relative_date == "今日":
        return today.strftime("%Y-%m-%d")
    elif relative_date == "昨日":
        return (today - timedelta(days=5)).strftime("%Y-%m-%d")
    # 這邊改過了
    elif relative_date == "前天":
        return (today - timedelta(days=2)).strftime("%Y-%m-%d")
    return None

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

def prepare_data():
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

def process_data(persons, absolute_dates, df):
    if persons and absolute_dates:
        df_filtered = df[(df["姓名"] == persons) & (df["日期"] == absolute_dates)]
        if not df_filtered.empty:
            df_filtered_replaced = df_filtered.replace({"姓名": {persons[0]: "xxx"}})
            return df_filtered_replaced.to_string(index=False)
    return None

# 主要處理函數
def NERAG(text):
    DB = ["生命跡象", "護理紀錄"]
    df = prepare_data()

    results = predict_and_extract_entities(text, tokenizer, model)
    keywords = extract_keywords(text, DB)
    persons = extract_entities(results, "PERSON")
    dates = extract_entities(results, "DATE")
    absolute_dates = [relative_date_to_absolute(date) for date in dates if relative_date_to_absolute(date)]

    if not persons and not dates and not keywords:
        return "No PERSON, DATE, or DB found in the text."

    text_description = process_data(persons[0], absolute_dates[0], df)
    summary = generate_summary(text_description)

    if summary:
        return summary.replace("xxx", persons[0])
    return "Failed to generate summary."

# Gradio 界面
iface = gr.Interface(
    fn=NERAG,
    inputs="text",
    outputs="text",
    title="NER with Gradio",
    description="請輸入欲查詢的病患，ex: 常小芬昨日的生命跡象"
)

if __name__ == "__main__":
    iface.launch()