# 導入必要的庫
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from transformers import AutoTokenizer, AutoModel
from sentence_transformers import SentenceTransformer
import faiss
import os
import torch
import json
import logging
import google.generativeai as genai
from dotenv import load_dotenv

# 加載環境變量
load_dotenv()

# 初始化 FastAPI 應用
app = FastAPI()

# 設置日誌級別
logging.basicConfig(level=logging.INFO)

# 獲取環境變量
password = os.getenv('MONGODB_PASSWORD')
API_KEY = os.getenv('API_KEY')

# 設置 MongoDB 連接 URI
uri = f"mongodb+srv://ai-nerag:{password}@ai-nerag.iiltl.mongodb.net/?retryWrites=true&w=majority"

# 創建 MongoDB 客戶端並連接到服務器
client = MongoClient(uri)

# 確認 MongoDB 連接
try:
    client.admin.command('ping')
    print("成功連接到 MongoDB!")
except Exception as e:
    print(f"連接 MongoDB 時發生錯誤: {e}")

# 獲取數據庫和集合
db = client['release']
patients_collection = db["patients"]
vitalsigns_collection = db["vitalsigns"]
nursingnotes_collection = db["nursingnotes"]
nursingnotedetails_collection = db["nursingnotedetails"]
nursingdiagnoses_collection = db["nursingdiagnoses"]
nursingdiagnosisrecords_collection = db["nursingdiagnosisrecords"]

# 初始化 transformers 模型和分詞器
tokenizer = AutoTokenizer.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")
model = AutoModel.from_pretrained("emilyalsentzer/Bio_ClinicalBERT")

# 初始化句子轉換器
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


# 定義文本編碼函數
def encode_text(text, model, tokenizer, max_length=512):
    # 將輸入轉換為字符串
    if isinstance(text, dict):
        text = text.get('pattern') or text.get('info') or str(text)
    else:
        text = str(text)

    # 使用 tokenizer 處理文本
    inputs = tokenizer(text, padding=True, truncation=True, max_length=max_length, return_tensors="pt")

    # 使用模型生成嵌入
    with torch.no_grad():
        outputs = model(**inputs)

    # 返回最後一層隱藏狀態的平均值作為嵌入
    return outputs.last_hidden_state.mean(dim=1)

# 編碼知識庫
knowledge_embeddings = []
for item in knowledge_base:
    if isinstance(item, dict):
        text = item.get('pattern') or item.get('info')
    else:
        text = item
    if text:
        embedding = encode_text(text, model, tokenizer)
        knowledge_embeddings.append(embedding)

# 將嵌入轉換為一個大的張量
knowledge_embeddings = torch.cat(knowledge_embeddings, dim=0)

# 創建 FAISS 索引用於快速相似度搜索
dimension = knowledge_embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(knowledge_embeddings)

# 檢索相關知識的函數
def retrieve_relevant_knowledge(query, top_k=5):
    query_embedding = encode_text(query, model, tokenizer)
    similarities = torch.nn.functional.cosine_similarity(query_embedding, knowledge_embeddings)
    top_indices = similarities.argsort(descending=True)[:top_k]
    return [knowledge_base[i] for i in top_indices.tolist()]

# 配置 Google Generative AI
genai.configure(api_key=API_KEY)

# 生成 MongoDB 查詢的函數
def generate_mongo_query(query, retrieved_knowledge):
    model = genai.GenerativeModel('gemini-pro')

    # 將檢索到的知識轉換為字符串
    knowledge_str = ""
    for item in retrieved_knowledge:
        if isinstance(item, dict):
            knowledge_str += item.get('pattern', '') + " " + item.get('info', '') + "\n"
        elif isinstance(item, str):
            knowledge_str += item + "\n"
        else:
            knowledge_str += str(item) + "\n"

    prompt = f"""
    根據以下自然語言查詢和相關知識，生成一個有效的MongoDB查詢：

    查詢: {query}

    相關知識:
    {knowledge_str}

    請生成一個有效的MongoDB查詢，不要包含任何其他解釋。
    """

    system_prompt = "你是一個專門將自然語言轉換為MongoDB查詢的助手。"
    response = model.generate_content([system_prompt, prompt])
    return response.text

# 確定要使用哪個 MongoDB 集合的函數
def determine_collection(query_dict):
    if 'patient_id' in query_dict:
        return 'patients'
    elif 'vital_signs' in query_dict:
        return 'vitalsigns'
    elif 'nursing_notes' in query_dict:
        return 'nursingnotes'
    else:
        return 'default_collection'

# 執行 MongoDB 查詢的函數
def execute_mongo_query(query_string):
    try:
        query_dict = json.loads(query_string)
        collection_name = determine_collection(query_dict)

        if '$lookup' in query_string or '$aggregate' in query_string:
            result = list(db[collection_name].aggregate(query_dict))
        else:
            result = list(db[collection_name].find(query_dict))

        for doc in result:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        return result
    except json.JSONDecodeError:
        return {"error": "無法解析生成的查詢"}
    except Exception as e:
        return {"error": f"執行查詢時發生錯誤: {str(e)}"}

# 定義查詢模型
class Query(BaseModel):
    query: str

# FastAPI 路由處理函數
@app.post("/process_query")
async def process_query(query: Query):
    try:
        logging.info(f"收到查詢: {query.query}")
        relevant_knowledge = retrieve_relevant_knowledge(query.query)
        logging.info(f"檢索到的相關知識: {relevant_knowledge}")
        mongo_query = generate_mongo_query(query.query, relevant_knowledge)
        logging.info(f"生成的 MongoDB 查詢: {mongo_query}")
        result = execute_mongo_query(mongo_query)
        logging.info(f"查詢結果: {result}")
        return {
            "original_query": query.query,
            "relevant_knowledge": relevant_knowledge,
            "mongo_query": mongo_query,
            "result": result
        }
    except Exception as e:
        logging.error(f"處理查詢時發生錯誤: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# 主函數
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)