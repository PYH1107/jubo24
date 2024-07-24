# jubo24
## `.env`
1. 在 `.env` 中新添
    `MONGODB_PASSWORD=password`
2. 在 main.py 中新添
```python
# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# update:
主要更新在 `pmain.py`
```


[Toc]
# Installation
```
pip install fastapi uvicorn pydantic pymongo transformers torch jieba python-dotenv requests bson
```

# Experiments
## 從 NER 模型提取字詞項目並進行資料庫搜尋
- **實驗目的**
    - 本實驗旨在使用命名實體識別（NER）技術從文本中提取姓名、日期，並利用這些實體在資料庫中進行搜尋。最終將串接大型語言模型（LLM）生成相關的文字敘述。

- **實驗步驟**
    - **資料準備**：
        - 連接 MongoDB 以提取資料
    - **載入 NER**
        - 中研院提供的中文NER模型和對應的tokenizer，用於後續的文本實體識別。
        - 分詞並提取資料
    - **資料庫搜尋**
        - 比對篩選出來的關鍵字及資料，選擇 collection 並進行搜尋
        - 將搜尋到的資料轉成 JSON 檔以利後續讀取
    - **串接 LLM 生成文字敘述**
        - 使用Gemini API，在先假設是＂王小明＂的數據，根據提取的數據生成摘要。
    - **定義API：POST路由，實現基本的服務端功能**
        - 使用 fast API 及 postman ，用以確認實驗結果
    - **執行 Code**：
        - `uvicorn pmain:app --reload`
        - 或 `python pmain.py`

- **實驗結果**
    1. **已完成功能**：
        1. 如果輸入兩個日期，自動判斷為時間區間
        2. 各種日期輸入的防呆
            - YYYY-MM-DD
            - MM-DD-YYYY
            - YYY-MM-DD (民國年)
        3. 可依文字判讀日期
            - 今天、昨天、前天、大前天
        4. 若資料庫中無輸入之姓名或者該住民無指定日期之資料，會告知使用者，避免 LLM 產出錯誤的資訊

    2. **輸入與輸出**：
##### 1) 以數字指定時間區段

- 輸入：
        ```{"text": "董小萱的生命跡象 2019-6-17 2019-6-19 "}```
- 輸出：
    - 第一次
    ```json
        {
        "from_date": "2019-06-17",
        "to_date": "2019-06-19",
        "person_names": [
            {
                "lastName": "董",
                "firstName": "小萱"
            }
        ],
        "keywords": [
            "生命跡象"
        ],
            "result": "在 2019 年 6 月 17 日，董小萱在 17:00 的體溫為 36.6 度，並於 19:00 上升至 37.8 度。接著在 21:00，董小萱的體溫進一步上升至 38.7 度，但在 22:10 小幅下降至 38.1 度。\n\n稍後，董小萱在 22:30 的體溫測量記錄中，除了體溫為 37.9 度外，還包括其他生理數據，如收縮壓 132 和舒張壓 84。\n\n到了 6 月 18 日清晨，董小萱的體溫降至 37.2 度。而在同日 21:41，其體溫回落至 36.7 度，舒張壓下降至 89，但脈搏率減慢至 85。\n\n隔天，6 月 19 日 22:03，董小萱的體溫穩定在 36.6 度，而舒張壓進一步下降至 68，脈搏率也維持在 82。"
        }
    ```
    - 第二次：
```json
    {
    "from_date": "2019-06-17",
    "to_date": "2019-06-19",
    "person_names": [
        {
            "lastName": "董",
            "firstName": "小萱"
        }
    ],
    "keywords": [
        "生命跡象"
    ],
        "result": "在 2019 年 6 月 17 日至 19 日期間，董小萱的體溫有記錄：\n\n* 2019-06-17T17:00:08.886000：36.6 度\n* 2019-06-17T19:00:28.453000：37.8 度\n* 2019-06-17T21:00:50.478000：38.7 度\n* 2019-06-17T22:10:16.284000：38.1 度\n* 2019-06-17T22:30:44：37.9 度\n* 2019-06-18T02:27:17.208000：37.2 度\n* 2019-06-18T21:41:24：36.7 度\n* 2019-06-19T22:03:59：36.6 度\n\n在 6 月 17 日 22:30 的記錄中，除了體溫外，還包含了其他生命徵象，包括：\n\n* 舒張壓：84 mmHg\n* 脈搏率：109 bpm\n* 呼吸頻率：20 cpm\n* 收縮壓：132 mmHg"
    }
```

##### 2)  以文字指定日期
        
- **輸入**
`{"text": "常小芬前天的護理紀錄 "}`

- 輸出
```json=
    {
    "from_date": "2024-07-22",
    "to_date": "2024-07-22",
    "person_names": [
        {
            "lastName": "常",
            "firstName": "小芬"
        }
    ],
    "keywords": [
        "生命跡象"
    ],
    "result": "常小芬的健康數據在 2024 年 7 月 22 日上午 6:17 測量如下：\n\n* 體溫：36.3°C\n* 脈搏率：53 次/分\n* 呼吸頻率：13 次/分\n* 收縮壓：13 mmHg\n* 舒張壓：83 mmHg\n* 血氧飽和度：93%\n\n這些數值均在正常範圍內，表明常小芬目前健康狀況良好。"}
```

##### 3) 以文字說明時間區段
- 輸入
    ```{"text": "常小芬前天到今天的生命跡象 "} ```

##### 4) 以民國年輸入
- 輸入
`{ "text": "常小芬 113-7-23 的生命跡象是甚麼 " }`

---
- 實驗結論
1. LLM 之不確定性
    此實驗通過提取輸入文本中的實體（人名、日期、關鍵字等），並根據這些實體從資料庫中查詢相關數據，然後生成自然語言的摘要描述。由「1) 以數字指定時間區段」所呈現之兩次輸出摘要可看出 LLM 給予的描述並不穩定。
2. 

## 程式碼
``` python=
from pymongo.mongo_client import MongoClient
#from pymongo.server_api import ServerApi
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
#from typing import List, Dict
from datetime import datetime, timedelta
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch
import re
import os
import jieba
import jieba.analyse
#import jieba.posseg as pseg
from bson import ObjectId
import json
import uvicorn
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv
import requests

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
    global first_name, last_name
    if len(full_name) > 1:
        last_name = full_name[0]
        first_name = full_name[1:]
        return {"lastName": last_name ,"firstName": first_name}
    else:
        return {"lastName": last_name ,"firstName": first_name}

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
        r'\b(\d{1,2})[-/](\d{1,2})[-/](\d{4})\b',  # MM-DD-YYYY or MM/DD/YYYY
        r'\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYYY-MM-DD or YYYY/MM/DD
        r'\b(\d{4})年(\d{1,2})月(\d{1,2})日\b',  # YYYY年MM月DD日
        r'\b(\d{1,2})月(\d{1,2})日(\d{4})年\b',  # MM月DD日YYYY年
        r'\b民國(\d{1,3})年(\d{1,2})月(\d{1,2})日\b',  # 民國YYY年MM月DD日
        r'\b(\d{2,3})[-/](\d{1,2})[-/](\d{1,2})\b',  # YYY-MM-DD or YYY/MM/DD (民國年)
    ]

    def relative_date_to_absolute(relative_date):
        today = datetime.today()
        if relative_date == "今天":
            return today.strftime("%Y-%m-%d")
        elif relative_date == "昨天" or relative_date == "昨日":
            return (today - timedelta(days=1)).strftime("%Y-%m-%d")
        elif relative_date == "大前天" or relative_date == "大前日":
            #print("大前天")
            return (today - timedelta(days=3)).strftime("%Y-%m-%d")
        elif relative_date == "前天" or relative_date == "前日":
            #print("前天")
            return (today - timedelta(days=2)).strftime("%Y-%m-%d")
        else:
            return None

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
                    elif len(groups[2]) == 4:  # MM-DD-YYYY
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
    
    # 處理相對日期
    relative_dates = ["今天", "昨天", "昨日", "大前天", "大前日", "前天", "前日"]
    processed_text = text
    for rel_date in relative_dates:
        if rel_date in processed_text:
            abs_date = relative_date_to_absolute(rel_date)
            if abs_date and abs_date not in dates:
                dates.append(abs_date)
            processed_text = processed_text.replace(rel_date, '')  # 移除已處理的日期

    global from_date, to_date
    dates = sorted(dates)  # 按日期排序
    if len(dates) == 1:
        from_date = dates[0]
        to_date = dates[0]
        dates = [from_date, to_date]
    elif len(dates) > 1:
        from_date = dates[0]
        to_date = dates[-1]
        dates = [from_date, to_date]

    return dates


# 自訂 JSON 編碼器
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super(JSONEncoder, self).default(o)

# 過濾空欄位
def filter_empty_fields(doc):
    return {k: v for k, v in doc.items() if v}

def read_patients_info():
    query = {"lastName": last_name, "firstName": first_name}
    documents = patients_collection.find(query)
    if patients_collection.count_documents(query) == 0:
        print("Not find patient name.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            #print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            return doc["_id"]

def read_vital_signs(patient_id, start_date, end_date):
    start_datetime = datetime.strptime(start_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": start_datetime,
            "$lte": end_datetime
        }
    }
    projection = {"PR": 1, "RR": 1, "SYS": 1, "TP": 1, "DIA": 1, "SPO2": 1, "PAIN": 1, "createdDate": 1, "_id": 0}  # 投影指定欄位
    documents = vitalsigns_collection.find(query, projection)
    text_description = []
    if vitalsigns_collection.count_documents(query) == 0:
        print("read_vital_signs not find.")
    else:

        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            key_mapping = {
                "PR": "脈搏率",
                "SYS": "收縮壓",
                "DIA": "舒張壓",
                "SPO2": "血氧飽和度",
                "TP": "體溫",
                "RR": "呼吸頻率",
                "createdDate": "記錄時間"
            }
            filtered_doc = {key_mapping.get(k, k): v for k, v in filtered_doc.items()}
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            temp = json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder)
            text_description.append(temp)

    return text_description

def read_nursingnote(patient_id, start_date, end_date):
    start_datetime = datetime.strptime(start_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": start_datetime,
            "$lte": end_datetime
        }
    }
    projection = {"_id": 1, "focus": 1, "createdDate": 1}
    documents = nursingnotes_collection.find(query, projection)
    text_description = []
    if nursingnotes_collection.count_documents(query) == 0:
        print("read_nursingnote not find.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            temp = json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder)
            text_description.append(temp)
    return text_description

def read_nursingnotedetails(patient_id, start_date, end_date):
    start_datetime = datetime.strptime(start_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": start_datetime,
            "$lte": end_datetime
        }
    }
    projection = {"_id": 0, "content": 1, "createdDate": 1}
    documents = nursingnotedetails_collection.find(query, projection)
    text_description = []
    if nursingnotedetails_collection.count_documents(query) == 0:
        print("read_nursingnotedetails not find.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            temp = json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder)
            text_description.append(temp)
    return text_description

def read_nursingdiagnoses(patient_id, start_date, end_date):
    start_datetime = datetime.strptime(start_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": start_datetime,
            "$lte": end_datetime
        }
    }
    projection = {"_id": 0, "features": 1, "createdDate": 1, "goals": 1, "plans": 1, "attr": 1}
    documents = nursingdiagnoses_collection.find(query, projection)
    text_description = []
    if nursingdiagnoses_collection.count_documents(query) == 0:
        print("read_nursingdiagnoses not find.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            temp = json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder)
            text_description.append(temp)
    return text_description

def read_nursingdiagnosisrecords(patient_id, start_date, end_date):
    start_datetime = datetime.strptime(start_date + " 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    query = {
        "patient": ObjectId(patient_id),
        "createdDate": {
            "$gte": start_datetime,
            "$lte": end_datetime
        }
    }
    projection = {"_id": 0, "evaluation": 1, "goals": 1, "createdDate": 1}
    documents = nursingdiagnosisrecords_collection.find(query, projection)
    text_description = []
    if nursingdiagnosisrecords_collection.count_documents(query) == 0:
        print("read_nursingdiagnosisrecords not find.")
    else:
        for doc in documents:
            filtered_doc = filter_empty_fields(doc)
            print(json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder))
            temp = json.dumps(filtered_doc, ensure_ascii=False, indent=4, cls=JSONEncoder)
            text_description.append(temp)
    return text_description


def generate_summary(text_description, start_date, end_date):
    url = f'https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={API_KEY}'
    headers = {'Content-Type': 'application/json'}
    data = {
        "contents": [
            {
                "parts": [{"text": f"請為王小明的數據，要生成一個自然的摘要描述{{{text_description}}}"}]
            }
        ]
    }
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        return response.json()["candidates"][0]["content"]["parts"][0]["text"]
    return None


# generate responses
def NERAG(text):
    results = predict_and_extract_entities(text, tokenizer, model) # 分詞提取的結果
    person_names = extract_entities(results, 'PER') # 從 NER 中 得到人名
    dates = extract_date(text)# 從 NER 中 得到日期
    keywords = extract_keywords(text, DB) #得到關鍵字
    person_names_str = ", ".join(person_names)
    print("person_names:" + person_names_str)

    if len(dates) >= 2:
        start_date, end_date = dates[0], dates[-1]
    elif len(dates) == 1:
        start_date = end_date = dates[0]
    else:
        return "No valid date found in the text."
    
    patient_id = read_patients_info()
    if patient_id:
        text_description = []
        if "生命跡象" in keywords:
            text_description.extend(read_vital_signs(patient_id, dates[0], dates[1]))
        if "護理紀錄" in keywords:
            text_description.extend(read_nursingnote(patient_id, dates[0], dates[1]))
            text_description.extend(read_nursingnotedetails(patient_id, dates[0], dates[1]))
            text_description.extend(read_nursingdiagnoses(patient_id, dates[0], dates[1]))
            text_description.extend(read_nursingdiagnosisrecords(patient_id, dates[0], dates[1]))
        if not text_description :
            print("All info not find")
            return "All patient info does not find in date range."
        else:
            # 生成摘要
            summary = generate_summary(text_description, start_date, end_date)
            if summary:
                summary = summary.replace("王小明", last_name + first_name)
                print("summary="+summary)
                return summary
            return "Failed to generate summary."
    else:
        print("Not find patient_id in NERAG.")
        return "Not find patient_id"







@app.get("/", response_class=HTMLResponse)
async def get_home():
    with open("index.html", "r", encoding="utf-8") as file:
        html_content = file.read()
    return HTMLResponse(content=html_content)


@app.post("/summary")
async def api_extract_entities(input: TextInput):
    if not input.text:
        raise HTTPException(status_code=400, detail="Text input is required")

    results = predict_and_extract_entities(input.text, tokenizer, model)
    person_names = extract_entities(results, 'PER')
    print("input:"+ input.text)
    #dates = extract_date(input.text)
    keywords = extract_keywords(input.text, DB)

    name_parts = [extract_name_parts(name) for name in person_names]

    result = NERAG(input.text)

    if "Failed to generate summary" in result:
        raise HTTPException(status_code=404, detail="Failed to generate summary or no data found.")

    return {
        "from_date": from_date,
        "to_date": to_date,
        "person_names": name_parts,
        "keywords": keywords,
        "result": result
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
```
