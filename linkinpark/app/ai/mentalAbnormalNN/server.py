import os
from time import perf_counter

import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from linkinpark.lib.ai.mentalAbnormalNN.model import (
    update_model,
    render_result,
    act_abnormal_detection,
    beh_abnormal_detection,
)
from linkinpark.lib.ai.mentalAbnormalNN.utils import ActContent, BevContent
from linkinpark.lib.common import logger
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware

app = FastAPI(
    title="Mental abnormal detection for NN project.",
    description=open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        "r"
    ).read(),
    version="1.0.0",
)
app.add_middleware(FastAPIMiddleware,
                   path_prefix="/ai-mental-abnormal-detection")


@app.get("/ai-mental-abnormal-detection/")
async def root():
    return RedirectResponse("/docs")


@app.post("/ai-mental-abnormal-detection/activityAbnormal")
async def detect_activity(act_content: ActContent):
    pre_st = perf_counter()
    try:
        result = act_abnormal_detection(act_content)
        response = render_result(act_content, result, "activityAbnormal")
    except Exception as e:
        logger.error(
            msg={"message": "Failed to detect activity abnormality.",
                 "error": e}
        )
        raise SystemError(e)
    pre_ed = perf_counter()
    sec = round(pre_ed - pre_st, 4)
    logger.info(
        msg={
            "message": f"Latency for detecting activity abnormal is {sec}'s",
            "metrics": {"activity_abnormal_latency": sec},
        }
    )
    return response


@app.post("/ai-mental-abnormal-detection/behaviourAbnormal")
async def detect_behaviour(beh_content: BevContent):
    pre_st = perf_counter()
    try:
        result = beh_abnormal_detection(beh_content)
        response = render_result(beh_content, result, "behaviourAbnormal")
    except Exception as e:
        logger.error(
            msg={"message": "Failed to detect behavioural abnormality.",
                 "error": e}
        )
        raise SystemError(e)
    pre_ed = perf_counter()
    sec = round(pre_ed - pre_st, 4)
    logger.info(
        msg={
            "message": f"Latency for detecting behaviour abnormal is {sec}'s",
            "metrics": {"behaviour_abnormal_latency": sec},
        }
    )
    return response


@app.get("/ai-mental-abnormal-detection/update")
async def load_model():
    pre_st = perf_counter()
    update_model()
    pre_ed = perf_counter()
    sec = round(pre_ed - pre_st, 4)
    logger.info(
        msg={
            "message": f"Loading model spent {sec}'s",
            "metrics": {"load_model_latency": sec},
        }
    )


def main():
    uvicorn.run(
        "linkinpark.app.ai.mentalAbnormalNN.server:app",
        host="0.0.0.0",
        port=5000,
        proxy_headers=True,
    )


if __name__ == "__main__":
    main()
