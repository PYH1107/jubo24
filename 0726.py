from fastapi import FastAPI
import uvicorn
from pymongo import MongoClient
from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer

import os
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv
import numpy as np
from transformers import AutoTokenizer, AutoModel
import torch

#1: 環境
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



# 初始化 sentence transformer
tokenizer = AutoTokenizer.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
model = AutoModel.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")

encoder = SentenceTransformer('all-MiniLM-L6-v2')

# 準備知識庫
knowledge_base = [
    # 單一病人特定數據查詢
    {
        "pattern": "查詢 [病人姓名] 的最新 [數據類型]",
        "mongo_query": "db.患者集合.aggregate([{$match: {姓名: '[病人姓名]'}}, {$lookup: {from: '數據集合', localField: '_id', foreignField: 'patient_id', as: '數據'}}, {$unwind: '$數據'}, {$sort: {'數據.時間': -1}}, {$limit: 1}])",
        "example": "查詢王小明的最新血壓數據"
    },
    {
        "pattern": "查詢 [病人姓名] 的 [數據類型] 在 [時間] 的記錄",
        "mongo_query": "db.患者集合.aggregate([{$match: {姓名: '[病人姓名]'}}, {$lookup: {from: '數據集合', localField: '_id', foreignField: 'patient_id', as: '數據'}}, {$unwind: '$數據'}, {$match: {'數據.類型': '[數據類型]', '數據.時間': {$gte: ISODate('[時間開始]'), $lte: ISODate('[時間結束]')}}])",
        "example": "查詢李四的血糖數據在過去24小時的記錄"
    },

    # 護理紀錄歷史查詢
    {
        "pattern": "[病人姓名] 在 [時間] 的 [數據類型] 有哪些重要事項",
        "mongo_query": "db.護理紀錄.find({patient_id: ObjectId('...'), 時間: {$gte: ISODate('[時間開始]'), $lte: ISODate('[時間結束]')}, 類型: '[數據類型]'}).sort({時間: -1})",
        "example": "張三在過去一週的護理紀錄有哪些重要事項"
    },

    # 特定護理項目查詢
    {
        "pattern": "提供 [病人姓名] 的 [特定護理項目] 記錄",
        "mongo_query": "db.特定護理項目.find({patient_id: ObjectId('...'), 項目類型: '[特定護理項目]'}).sort({時間: -1})",
        "example": "提供王小明的藥物過敏紀錄"
    },

    # 多名病人或全部病人的數據查詢
    {
        "pattern": "查詢所有病人的 [數據類型]",
        "mongo_query": "db.患者集合.aggregate([{$lookup: {from: '數據集合', localField: '_id', foreignField: 'patient_id', as: '數據'}}, {$unwind: '$數據'}, {$match: {'數據.類型': '[數據類型]'}}, {$sort: {'數據.時間': -1}}, {$group: {_id: '$_id', 最新數據: {$first: '$數據'}}}])",
        "example": "查詢所有病人的最新體溫情況"
    },

    # 特定護理情況查詢
    {
        "pattern": "[病人姓名] 的 [數據類型] 中是否有 [特定護理項目]",
        "mongo_query": "db.護理紀錄.find({patient_id: ObjectId('...'), 類型: '[數據類型]', 內容: {$regex: '[特定護理項目]'}})",
        "example": "王小明的護理紀錄中是否有寫到跌倒風險"
    },

    # MongoDB 查詢相關
    {
        "info": "MongoDB使用find()方法進行查詢。"
    },
    {
        "info": "使用$gt運算符表示大於。"
    },
    {
        "info": "使用$lt運算符表示小於。"
    },
    {
        "info": "使用$gte運算符表示大於等於。"
    },
    {
        "info": "使用$lte運算符表示小於等於。"
    },
    {
        "info": "使用$eq運算符表示等於。"
    },
    {
        "info": "使用$ne運算符表示不等於。"
    },
    {
        "info": "使用$in運算符表示在指定數組中。"
    },
    {
        "info": "使用$nin運算符表示不在指定數組中。"
    },
    {
        "info": "使用$and運算符表示與操作。"
    },
    {
        "info": "使用$or運算符表示或操作。"
    },
    {
        "info": "使用$exists運算符檢查欄位是否存在。"
    },
    {
        "info": "使用$type運算符檢查欄位類型。"
    },
    {
        "info": "使用$regex運算符進行正則表達式匹配。"
    },
    {
        "info": "使用$sort進行排序，1表示升序，-1表示降序。"
    },
    {
        "info": "使用$limit限制返回的文檔數量。"
    }
]

# # 使用 SentenceTransformer 將知識庫轉換為向量
def encode_text(text, model, tokenizer, max_length=512):
    if isinstance(text, dict):
        # 如果是字典，提取需要的字段
        text = text.get('pattern') or text.get('info') or str(text)
    else:
        text = str(text)

    inputs = tokenizer(text, padding=True, truncation=True, max_length=max_length, return_tensors="pt")

    with torch.no_grad():
        outputs = model(**inputs)

    embeddings = outputs.last_hidden_state.mean(dim=1)
    return embeddings

# 編碼知識庫
knowledge_embeddings = []
for item in knowledge_base:
    if isinstance(item, dict):
        # 假設我們想要編碼 'pattern' 或 'info' 字段
        text = item.get('pattern') or item.get('info')
    else:
        text = item
    if text:
        embedding = encode_text(text, model, tokenizer)
        knowledge_embeddings.append(embedding)

# 將嵌入轉換為一個大的張量
knowledge_embeddings = torch.cat(knowledge_embeddings, dim=0)



# 創建 FAISS 索引
dimension = knowledge_embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(knowledge_embeddings)


def retrieve_relevant_knowledge(query, top_k=5):
    query_embedding = encode_text(query, model, tokenizer)

    similarities = torch.nn.functional.cosine_similarity(query_embedding, knowledge_embeddings)
    top_indices = similarities.argsort(descending=True)[:top_k]

    return [knowledge_base[i] for i in top_indices.tolist()]


import google.generativeai as genai
genai.configure(api_key="{API_KEY}")
def generate_mongo_query(query, retrieved_knowledge):
    # 初始化模型
    model = genai.GenerativeModel('gemini-pro')

    prompt = f"""
    根據以下自然語言查詢和相關知識，生成一個有效的MongoDB查詢：

    查詢: {query}

    相關知識:
    {' '.join(retrieved_knowledge)}

    請生成一個有效的MongoDB查詢，不要包含任何其他解釋。
    """

    # 系統提示
    system_prompt = "你是一個專門將自然語言轉換為MongoDB查詢的助手。"

    # 生成內容
    response = model.generate_content([system_prompt, prompt])

    # 返回生成的內容
    return response.text

# 使用示例
# query = "查詢王小明的最新血壓數據"
# retrieved_knowledge = ["MongoDB使用find()方法進行查詢。", "使用$sort進行排序，1表示升序，-1表示降序。"]
# result = generate_mongo_query(query, retrieved_knowledge)
# print(result)

# 處理用戶查詢
query = "如何在 MongoDB 中查找大於某個值的記錄？"
relevant_knowledge = retrieve_relevant_knowledge(query)
print(relevant_knowledge)




import json
from pymongo import MongoClient

# 假設您已經建立了 MongoDB 連接
client = MongoClient('your_mongodb_uri')
db = client['your_database_name']

def determine_collection(query_dict):
    # 這個函數根據查詢內容決定使用哪個集合
    # 您需要根據您的具體需求來實現這個邏輯
    if 'patient_id' in query_dict:
        return 'patients'
    elif 'vital_signs' in query_dict:
        return 'vitalsigns'
    elif 'nursing_notes' in query_dict:
        return 'nursingnotes'
    # 添加更多的條件來匹配其他集合
    else:
        return 'default_collection'  # 如果無法確定，使用默認集合

def execute_mongo_query(query_string):
    try:
        # 解析查詢字符串
        query_dict = json.loads(query_string)

        # 決定使用哪個集合
        collection_name = determine_collection(query_dict)

        # 檢查是否是聚合查詢
        if '$lookup' in query_string or '$aggregate' in query_string:
            # 執行聚合查詢
            pipeline = query_dict
            result = list(db[collection_name].aggregate(pipeline))
        else:
            # 執行普通查詢
            result = list(db[collection_name].find(query_dict))

        # 處理結果中的 ObjectId
        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        return result
    except json.JSONDecodeError:
        return {"error": "無法解析生成的查詢"}
    except Exception as e:
        return {"error": f"執行查詢時發生錯誤: {str(e)}"}

# 使用示例
# query_string = '{"patient_id": "12345", "vital_signs": {"$exists": true}}'
# result = execute_mongo_query(query_string)
# print(result)

def process_natural_language_query(query):
    # 檢索相關知識
    relevant_knowledge = retrieve_relevant_knowledge(query)

    # 生成 MongoDB 查詢
    mongo_query_string = generate_mongo_query(query, relevant_knowledge)

    # 執行查詢
    result = execute_mongo_query(mongo_query_string)

    return {
        "original_query": query,
        "generated_mongo_query": mongo_query_string,
        "result": result
    }

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging

app = FastAPI()
logging.basicConfig(level=logging.INFO)

class Query(BaseModel):
    query: str


@app.post("/process_query")
async def process_query(query: Query):
    try:
        logging.info(f"Received query: {query.query}")
        relevant_knowledge = retrieve_relevant_knowledge(query.query)
        logging.info(f"Retrieved relevant knowledge: {relevant_knowledge}")
        mongo_query = generate_mongo_query(query.query, relevant_knowledge)
        logging.info(f"Generated MongoDB query: {mongo_query}")
        result = execute_mongo_query(mongo_query)
        logging.info(f"Query result: {result}")
        return {
            "original_query": query.query,
            "relevant_knowledge": relevant_knowledge,
            "mongo_query": mongo_query,
            "result": result
        }
    except Exception as e:
        logging.error(f"Error processing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

'''
# 如果需要,您可以添加更多端點
@app.get("/")
async def root():
    return {"message": "Welcome to the Natural Language to MongoDB Query API"}
'''

