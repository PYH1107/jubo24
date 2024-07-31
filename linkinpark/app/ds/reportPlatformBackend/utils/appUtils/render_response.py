from datetime import datetime
from urllib.parse import quote

import pytz
from bson import ObjectId
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from linkinpark.app.ds.reportPlatformBackend.utils import (
    get_org_name,
    get_report,
    get_report_docstring,
    search_id_by_name,
    search_org_by_nickname)


def get_params(query):
    body = query.dict()
    body["org"] = ObjectId(body["org"])
    return body


def render_display_period(start, end):
    if start == end:
        format_period = start.strftime("%Y%m%d")
    else:
        format_period = start.strftime("%Y%m%d") + "-" + end.strftime("%Y%m%d")
    return format_period


def get_generate_date(time_format="%Y%m%d"):
    utc_tz, taiwan_tz = pytz.utc, pytz.timezone('Asia/Taipei')
    utc_today = utc_tz.localize(datetime.utcnow())
    generate_date = utc_today.astimezone(taiwan_tz)
    return generate_date.strftime(time_format)


def get_file_name(report_name, params):
    period = render_display_period(params["start"], params["end"])
    report_name = get_report_docstring("ReportName")[report_name]
    org_name = get_org_name(params["org"])
    generate_date = get_generate_date()
    file_name = f"{period}_{report_name}_{org_name}({generate_date})"
    return file_name


def render_file_response(report_name, query):
    report_name, params = report_name.name, get_params(query)
    file = get_report(report_name, params)
    file_name = quote(get_file_name(report_name, params))
    headers = {'Content-Disposition': f'inline; filename="{file_name}.xlsx"'}
    file_type = ("application/vnd.openxmlformats-officedocument."
                 "spreadsheetml.sheet")
    return Response(content=file, headers=headers, media_type=file_type)


def render_error_message(report_name, query, url, err_msg):
    name, params = report_name.name, get_params(query)
    report_name = get_report_docstring("ReportName")[name]
    poc_name = get_report_docstring("POC")[name]
    timestamp = get_generate_date("%Y-%m-%d %H:%M:%S.%f %z")
    return_msg = {
        "text": (
            f"Failed to generate report {report_name}\n"
            f"Please contact {poc_name} and provide the following "
            f"content.\n"
            f"{{\n"
            f"    URL: {url},"
            f"    Time: {timestamp},"
            f"    Content: {str(params)},"
            f"    Remark: {err_msg}"
            f"}}\n"
        )
    }
    return_msg = jsonable_encoder(return_msg)
    return JSONResponse(return_msg, status_code=422)


def render_nickname_search_result(nickname):
    res = search_org_by_nickname(nickname)
    res = jsonable_encoder(res)
    return JSONResponse(res)


def render_id_search_result(name):
    res = search_id_by_name(name)
    res = jsonable_encoder(res)
    return JSONResponse(res)


def render_report_list():
    res = get_report_docstring("ReportName")
    res = jsonable_encoder(res)
    return JSONResponse(res)
