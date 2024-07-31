import os
from time import perf_counter

import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from linkinpark.lib.ai.activityRecommendationNN.model import log_result, \
    render_result
from linkinpark.lib.ai.activityRecommendationNN.utils import Content, LOG, \
    LogInfo
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware

app = FastAPI(
    title="Activity recommendation for NN project.",
    description=open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        "r").read(),
    version="1.0.0",
)
app.add_middleware(FastAPIMiddleware)


@app.get("/")
async def root():
    return RedirectResponse("/docs")


@app.post("/predict")
async def predict(content: Content):
    pre_st = perf_counter()
    try:
        result = render_result(content)
    except Exception as e:
        LOG.error(msg={
            "message": "Failed to make predictions.",
            "error": e})
        raise SystemError(e)
    pre_ed = perf_counter()
    sec = round(pre_ed - pre_st, 4)
    LOG.info(msg={
        "message": f"Latency for making the prediction is {sec}'s",
        "metrics": {"predict_latency": sec}})
    return result


@app.post("/clicked")
async def clicked(log_info: LogInfo):
    result = log_result(log_info)
    return result


def main():
    uvicorn.run("linkinpark.app.ai.activityRecommendationNN.server:app",
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == "__main__":
    main()
