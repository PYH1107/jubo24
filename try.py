from fastapi import FastAPI
import uvicorn
from pymongo import MongoClient
from transformers import AutoTokenizer, AutoModel
import requests
import torch
import json
from bson import ObjectId
#import openai
from dotenv import load_dotenv
import os
import google.generativeai as genai
from pydantic import BaseModel
from transformers import AutoTokenizer, AutoModel
import torch


app = FastAPI()
# Load environment variables
load_dotenv()

app = FastAPI()
password = os.getenv('MONGODB_PASSWORD')
API_KEY = os.getenv('API_KEY')
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
nursingnotes_collection = db["nursingnotes"]
nursingnotedetails_collection = db["nursingnotedetails"]
nursingdiagnoses_collection = db["nursingdiagnoses"]
nursingdiagnosisrecords_collection = db["nursingdiagnosisrecords"]

# LLM 模型
tokenizer = AutoTokenizer.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
model = AutoModel.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")

# ---

# Knowledge Base
knowledge_base = [
    # MongoDB 查詢相關
    "MongoDB使用find()方法進行查詢。",
    "使用$gt運算符表示大於。",
    "使用$lt運算符表示小於。",
    "使用$gte運算符表示大於等於。",
    "使用$lte運算符表示小於等於。",
    "使用$eq運算符表示等於。",
    "使用$ne運算符表示不等於。",
    "使用$in運算符表示在指定數組中。",
    "使用$nin運算符表示不在指定數組中。",
    "使用$and運算符表示與操作。",
    "使用$or運算符表示或操作。",
    "使用$exists運算符檢查欄位是否存在。",
    "使用$type運算符檢查欄位類型。",
    "使用$regex運算符進行正則表達式匹配。",
    "使用$sort進行排序，1表示升序，-1表示降序。",
    "使用$limit限制返回的文檔數量。",

    # 數據結構示例
    "患者記錄結構f：{'姓名': 'string', '年齡': 'int', '性別': 'string', '病歷號': 'string'}",
    "生命徵象記錄結構：{'患者id': 'string', '時間': 'date', '體溫': 'float', '脈搏': 'int', '呼吸': 'int', '血壓': 'object'}",
    # ... (其他數據結構)

    # 查詢模式示例
    "查詢特定患者的最新護理記錄：db.nursingNotes.find({'患者id': patientId}).sort({'時間': -1}).limit(1)",
    "查詢特定患者的血壓數據：db.vitalSigns.find({'患者id': patientId, '血壓': {$exists: true}})",
    # ... (其他查詢模式)

    # 系統特定信息
    "我們的系統使用'patient_id'作為患者唯一標識符。",
    "所有時間戳都以ISO格式存儲。",
    "護理記錄分為以下類型：日常觀察、用藥記錄、治療過程、評估結果。",
]

# 實體類型
entity_types = {
    "病人姓名": ["例如：王小明、張三、李四等"],
    "數據類型": ["護理紀錄", "血壓數據", "心跳紀錄", "呼吸頻率", "血糖數據", "脈搏數據", "生命跡象數據",
                "皮膚狀況紀錄", "排便紀錄", "活動量", "疼痛評估紀錄", "睡眠紀錄", "體重變化", "護理異常紀錄",
                "用藥紀錄", "輸液紀錄", "飲食紀錄", "護理計畫", "藥物過敏紀錄", "體溫情況", "飲水紀錄"],
    "時間": ["昨天", "前天", "大前天", "今天早上", "過去一週", "本月", "這兩天", "過去24小時", "這個星期"],
    "特定護理項目": ["護理目標", "注射", "內容", "跌倒風險", "異常項目", "特別注意事項", "輸液時間"]
}

def identify_entities(query):
    inputs = tokenizer(query, return_tensors="pt", padding=True, truncation=True, max_length=512)
    outputs = model(**inputs)

    # 這裡使用last_hidden_state作為特徵
    features = outputs.last_hidden_state.squeeze().detach().numpy()

    # 簡單的實體識別邏輯（這裡需要更複雜的邏輯來實現真正的NER）
    entities = {}
    for entity_type, examples in entity_types.items():
        for example in examples:
            if example in query:
                entities[entity_type] = example
                break

    return entities
'''
def generate_mongo_query(natural_language_query, entities):
    prompt = f"""
    根據以下自然語言查詢和識別出的實體,生成一個MongoDB查詢:

    自然語言查詢: {natural_language_query}
    識別出的實體: {json.dumps(entities, ensure_ascii=False)}

    知識庫:
    {json.dumps(knowledge_base, ensure_ascii=False)}

    請生成一個有效的MongoDB查詢字典。
    """

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "你是一個專門將自然語言轉換為MongoDB查詢的助手。"},
            {"role": "user", "content": prompt}
        ]
    )

    mongo_query = json.loads(response.choices[0].message.content)
    return mongo_query
'''

def generate_mongo_query(natural_language_query, entities):
    prompt = f"""
    根據以下自然語言查詢和識別出的實體,生成一個MongoDB查詢:

    自然語言查詢: {natural_language_query}
    識別出的實體: {json.dumps(entities, ensure_ascii=False)}

    知識庫:
    {json.dumps(knowledge_base, ensure_ascii=False)}

    請生成一個有效的MongoDB查詢字典。僅返回 JSON 格式的查詢字典，不要包含任何其他解釋。
    """

    response = model.generate_content(prompt)

    try:
        # 嘗試解析回應為JSON
        mongo_query = json.loads(response.text)
    except json.JSONDecodeError:
        # 如果解析失敗，返回一個空查詢
        print("無法解析Gemini的回應為有效的JSON。回應內容：", response.text)
        mongo_query = {}

    return mongo_query

def execute_mongo_query(mongo_query):
    # 根據查詢內容決定使用哪個集合
    if "患者" in mongo_query:
        result = list(patients_collection.find(mongo_query))
    elif "生命徵象" in mongo_query:
        result = list(vitalsigns_collection.find(mongo_query))
    elif "護理記錄" in mongo_query:
        result = list(nursingnotes_collection.find(mongo_query))
    else:
        result = list(db.command("aggregate", "patients", pipeline=[mongo_query]))

    # 轉換 ObjectId 為字符串
    for doc in result:
        doc['_id'] = str(doc['_id'])

    return result

def generate_summary(query_result):
    prompt = f"""
    根據以下查詢結果生成一個簡短的摘要:

    {json.dumps(query_result, ensure_ascii=False)}

    摘要應該包含關鍵信息,並且易於理解。
    """

    response = model.generate_content(prompt)
    return response.text


class QueryRequest(BaseModel):
    query: str

def process_nursing_query(natural_language_query):
    entities = identify_entities(natural_language_query)
    mongo_query = generate_mongo_query(natural_language_query, entities)
    query_result = execute_mongo_query(mongo_query)
    summary = generate_summary(query_result)

    return {
        "original_query": natural_language_query,
        "entities": entities,
        "mongo_query": mongo_query,
        "query_result": query_result,
        "summary": summary
    }


@app.post("/process_query")
async def api_process_query(query: str):
    result = process_nursing_query(query)
    return result

@app.post("/generate_summary")
async def api_generate_summary(text: str):
    summary = generate_summary(text)
    return {"summary": summary}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)