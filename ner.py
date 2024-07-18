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

# 加載環境變量
load_dotenv()

# 從 .env 文件中讀取 MongoDB 連接字符串和資料庫名稱
URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# 連接到 MongoDB Atlas
print(f"Connecting to MongoDB using URI: {URI}")
client = MongoClient(URI)
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