import uvicorn
from typing import Optional, List
from pydantic import BaseModel
from fastapi import FastAPI, Request, HTTPException
from starlette_prometheus import metrics
from asgiref.sync import sync_to_async

from linkinpark.lib.ai.vitalsign.logger import logger
from linkinpark.lib.ai.vitalsign.utils import getTimestamp
from linkinpark.lib.ai.vitalsign.unsupervised import vsModel, vs
from linkinpark.lib.common.fastapi_monitor import PrometheusMiddleware
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware 

app = FastAPI()
app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=False)
app.add_middleware(FastAPIMiddleware)
app.add_route('/metrics', metrics)


class Result(BaseModel):
    __v: int
    createdDate: str
    importantItems: Optional[list] = None
    label: Optional[int] = None
    organization: Optional[str] = None
    patient: str
    rule: Optional[str] = None
    vitalsign: Optional[str] = None


class ItemList(BaseModel):
    result: List[Result]
    state: str


class Item(BaseModel):
    result: Result
    state: str


class Failed(BaseModel):
    message: str
    state: str


@app.get("/")
def health_check():
    return 200


@app.post("/predict")
async def predict(info: Request):
    vsData = await info.json()
    if isinstance(vsData, list):
        logger.info(vsData)
        logger.debug(getTimestamp() + ' Got ' +
                     str(len(vsData)) + ' vitalsigns!')
        try:
            res_list = []
            for data in vsData:
                res = await sync_to_async(vsModel)(data)
                result_obj = Result(**res)
                res_list.append(result_obj)
            return ItemList(result=res_list, state="success")
        except Exception as e:
            logger.error(vsData)
            logger.error(e)
            raise HTTPException(status_code=400, detail=str(e))

    elif isinstance(vsData, dict):
        try:
            logger.debug(getTimestamp() +
                         ' Got a single vitalsign!')
            logger.info(vsData)
            result = await sync_to_async(vsModel)(vsData)
            result_obj = Result(**result)
            return Item(result=result_obj, state="success")

        except Exception as e:
            logger.error(vsData)
            logger.error(e)
            raise HTTPException(status_code=400, detail=str(e))


def main():
    vs.load_org_model_into_memory()
    uvicorn.run("linkinpark.app.ai.vitalsign.server:app",
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == "__main__":
    main()
