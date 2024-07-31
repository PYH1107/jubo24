import os
import uvicorn
import requests
from datetime import datetime
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse

from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware


app = FastAPI(
    title="Extract VitalSign from video",
    description=open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"), "r"
    ).read(),
    version="1.0.0",
)
app.add_middleware(FastAPIMiddleware)


@app.post("/predict")
async def predict_vitalsign(file: UploadFile = File(...)):
    url = "http://125.228.205.202:8080/cv_api"
    current_time = datetime.now().strftime('%a %b %d %H:%M:%S %Y')
    
    data = {
        'DataInfoNo': 'NoAPI',
        'ClientTime': current_time,
        'DataGUID': 'yfkyt63zvx'
    }

    files = {
        'file': (file.filename, file.file)
    }

    response = requests.post(url, data=data, files=files)
    response_data = response.json()
    data_info = response_data['DataInfo']

    mapped_data = {
        "ID_NO": data_info.get("ID_NO", ""),
        "executiontime": data_info.get("Executiontime", ""),
        "cardiogram": data_info.get("HW", ""),
        "lacticAcid": data_info.get("LTv", ""),
        "RT": data_info.get("RT", ""),
        "spO2": data_info.get("S2", ""),
        "Situation": data_info.get("Situation", ""),
        "heartRate": data_info.get("bpm", ""),
        "bpSystolic": data_info.get("bpv1", ""),
        "bpDiastolic": data_info.get("bpv0", "")
    }

    return JSONResponse(content=mapped_data)


def main():
    uvicorn.run("linkinpark.app.ai.faceVitalsign.server:app", host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == '__main__':
    main()
