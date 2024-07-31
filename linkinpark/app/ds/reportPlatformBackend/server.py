import logging
import os
from time import perf_counter

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

from linkinpark.app.ds.reportPlatformBackend.utils import (
    FullName,
    NickName,
    Queries,
    ReportOptions,
    generate_description,
    render_error_message,
    render_file_response,
    render_id_search_result,
    render_nickname_search_result,
    render_report_list)
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware
from linkinpark.lib.common.logger import getLogger

ENV = os.getenv("APP_ENV", "dev")

app = FastAPI(
    title="Jubo Report Platform",
    description=generate_description(),
    version="3.0.a (Alpha testing)",
)
app.add_middleware(FastAPIMiddleware, path_prefix="/ds-report-platform")

log = getLogger(
    name="ds-report-platform",
    labels={
        "env": ENV,
        "app": "ds-report-platform"})


@app.get("/ds-report-platform/")
async def root():
    return RedirectResponse("/docs")


@app.post("/ds-report-platform/search_org")
async def search_org_by_nickname(nickname: NickName):
    return render_nickname_search_result(nickname)


@app.post("/ds-report-platform/search_id")
async def search_id_by_name(name: FullName):
    return render_id_search_result(name)


@app.post("/ds-report-platform/search_reports")
async def search_reports():
    return render_report_list()


@app.post("/ds-report-platform/{report_name}")
async def generate_report(
        report_name: ReportOptions, query: Queries, request: Request):
    start = perf_counter()
    try:
        res = render_file_response(report_name, query)
        log_lev, status, detail = logging.INFO, "SUCCESS", None
    except Exception as err_msg:
        res = render_error_message(report_name, query, request.url, err_msg)
        log_lev, status, detail = logging.ERROR, "FAILED", err_msg
    end = perf_counter()
    msg = (f"{status}: {query.user_id} generate {report_name.name} for"
           f" {query.org}. ({query.start} to {query.end})")
    msg = msg + str(detail) if detail else msg
    log_content = {
        "message": msg,
        "metrics": {
            "count": 1,
            "latency": end - start
        },
        "labels": {
            "_id": query.request_id,
            "status": status,
            "user": query.user_id,
            "report": report_name.name,
            "organization": query.org,
            "period_st": query.start,
            "period_ed": query.end,
            "suffix": query.suffix,
        }
    }
    print(bbb)
    log.debug(msg=str(log_content))
    for key, value in log_content["labels"].items():
        log.debug(msg=f"({key}, {value}): Value type {type(value)}")
        log_content["labels"][key] = str(value) if value is not None else ""
    log.log(log_lev, log_content)
    return res


def main():
    uvicorn.run("linkinpark.app.ds.reportPlatformBackend.server:app",
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == "__main__":
    main()
