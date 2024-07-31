import os
import uvicorn
from fastapi import FastAPI, UploadFile, File, responses
from pydantic import BaseModel

from linkinpark.lib.ai.voicefill.extract.re import ValueExtractor
from linkinpark.lib.ai.voicefill.stt.request_cht import chuSTT
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware


app = FastAPI(
    title="Extract VitalSign from audio",
    description=open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"), "r"
    ).read(),
    version="1.0.0",
)
app.add_middleware(FastAPIMiddleware)


def voice_processing(voice):
    AcSTT = chuSTT()
    res_json, voicetext = AcSTT.get_result(voice)
    return res_json, voicetext 


def text_processing(text):
    va = ValueExtractor()
    vitaltext = va.extract_value(text)
    note = va.extract_note(text)
    return {
        "vitalsign": vitaltext,
        "note": note
    }


class TextItem(BaseModel):
    word: str


@app.get("/")
async def root():
    return responses.RedirectResponse("/docs")


@app.post("/voice")
async def wav_extractor(wav_file: UploadFile = File(...)):
    contents = await wav_file.read()
    _, result = voice_processing(contents)
    return result


@app.post("/text")
async def text_extractor(text_data: TextItem):
    contents = text_data.word
    result = text_processing(contents)
    return result


@app.post("/predict")
async def full_extractor(wav_file: UploadFile = File(...)):
    contents = await wav_file.read()
    _, voicetext = voice_processing(contents)
    result = text_processing(voicetext)
    result['voicetext'] = voicetext
    return result
    

@app.post("/predict/connect")
async def connect():
    chustt = chuSTT()
    jobID = chustt.connect()
    return {"jobID": jobID}


@app.post("/predict/voice-recognize")
async def voice_recognize(file: UploadFile = File(...)):
    contents = await file.read()
    res_json, voicetext = voice_processing(contents)
    result = text_processing(voicetext)
    result['voicetext'] = voicetext
    return {
        "msg": "" if res_json['ResultStatus'] == 'Success' else res_json['ErrorMessage'],
        "isRecognitionDone": res_json["RecognitionDone"],
        "speechGot": res_json["SpeechGot"],
        "result": result
    }


@app.get("/predict/disconnect")
async def disconnect(jobID: str):
    chustt = chuSTT()
    chustt.disconnect(jobID)


def main():
    uvicorn.run("linkinpark.app.ai.voicefill.server:app", host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == '__main__':
    main()
