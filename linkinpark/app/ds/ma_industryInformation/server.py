import os
from typing import Optional

import httpx
import markdown
import uvicorn
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from linkinpark.app.ds.ma_industryInformation.ecpay import create_order
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware
from linkinpark.lib.common.logger import getLogger
from linkinpark.lib.common.postgres_connector import *

app = FastAPI()
app.add_middleware(FastAPIMiddleware, path_prefix="/ds-manage-assistant-info-web")
cwd = os.path.join("/", *(os.path.realpath(__file__).split(os.sep)[:-1]))

app.mount("/ds-manage-assistant-info-web/static", StaticFiles(
    directory=os.path.join(cwd, "static")), name="static")

templates = Jinja2Templates(
    directory=os.path.join(cwd, "templates"))

APP_NAME = os.getenv("APP_NAME", "ds-manage-assistant-info-web")
APP_ENV = os.getenv("APP_ENV", "dev")

CHANNEL_ID = os.getenv("CHANNEL_ID", "2001332747")
LIFF_POLICY_ID = os.getenv("LIFF_POLICY_ID", "2001332747-wzaxzQZ0")
LIFF_PAYMENT_ID = os.getenv("LIFF_PAYMENT_ID", "2001332747-p0dvmQWa")


logger = getLogger(
    name=APP_NAME,
    labels={
        "app": APP_NAME,
        "env": APP_ENV
    })


class UserProfileData(BaseModel):
    token: str
    os: str


class UserLabelData(BaseModel):
    user_id: Optional[str] = "test"
    click_time: str
    tag: str
    county: str


class UserLinkData(BaseModel):
    token: str
    click_time: str
    title: str
    link: str
    tag: str


class ECPayResponse(BaseModel):
    MerchantID: str
    RpHeader: dict
    TransCode: int
    TransMsg: str
    Data: dict


async def verify_line_user(token, client_id=CHANNEL_ID):
    url = "https://api.line.me/oauth2/v2.1/verify"
    async with httpx.AsyncClient() as client:
        response = await client.post(
            url,
            data={"id_token": token, "client_id": client_id},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return response


@app.post("/ds-manage-assistant-info-web/log_user_profile")
async def log_user_profile(item: UserProfileData):
    logger.info(f"{item}")
    response = await verify_line_user(item.token, client_id=CHANNEL_ID)
    if response.status_code == 200:
        profile_data = response.json()
        user_id = profile_data.get("sub")
        user_county = get_user_county(user_id)
        logger.info(
            f"User_id: {profile_data.get('sub')}, Display Name: "
            f"{profile_data.get('name')} enter LTC industry information "
            f"website",
            metrics={"click": 1},
            labels={"user_id": profile_data.get("sub"),
                    "name": profile_data.get("name"),
                    "channel_id": profile_data.get("aud"),
                    "platform": item.os,
                    'user_county': user_county}
        )
        return {"user_county": user_county}
    else:
        logger.info(
            f"response.status_code not 200(/log_user_profile): "
            f"{response.json()}",
            metrics={"fail": 1})


@app.post("/ds-manage-assistant-info-web/log_user_link")
async def log_user_link(item: UserLinkData):
    logger.info(f"{item}")
    print(f"Item: {item.token}", flush=True)
    response = await verify_line_user(item.token, client_id=CHANNEL_ID)
    if response.status_code == 200:
        profile_data = response.json()
        logger.info(
            f"{profile_data.get('sub')}/{profile_data.get('name')} clicks "
            f"{item.title} in category of {item.tag}.",
            metrics={"click": 1},
            labels={"user_id": profile_data.get("sub"),
                    "name": profile_data.get("name"),
                    "channel_id": profile_data.get("aud"),
                    "click_time": item.click_time,
                    "title": item.title,
                    "link": item.link,
                    "tag": item.tag}
        )
    else:
        logger.info(
            f"response.status_code not 200(/log_user_link): {response.json()}",
            metrics={"fail": 1})


def get_user_county(user_id: str):
    with PostgresConnectorFactory.get_cloudsql_postgres_connector(dbname="ma_bot", mode="prod") as conn:
        df = pd.DataFrame(conn.select_values("users"))

    user_county_df = df[df["user_id"] == user_id]
    if not user_county_df.empty:
        user_county = user_county_df.iloc[0]["county"]
        return user_county


@app.get("/ds-manage-assistant-info-web/search", response_class=HTMLResponse)
def generate_html_table(request: Request,
                        tag: list = Query(default=None),
                        county: list = Query(default=None),
                        liff_id=LIFF_POLICY_ID):
    with PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname="ma_bot", mode="prod") as conn:
        df = pd.DataFrame(conn.select_values("resource"))
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d").dt.date

    if tag is not None and county is not None:
        df = df[df["tag"].isin(tag) & df["county"].isin(county)]
    elif tag is not None:
        df = df[df["tag"].isin(tag)]
    elif county is not None:
        df = df[df["county"].isin(county)]

    month_ago = (datetime.today() - relativedelta(months=1)).date()
    seven_days_ago = (datetime.today() - timedelta(days=7)).date()
    df["seven_day"] = df["date"] >= seven_days_ago
    latest_data = df[df["date"] >= month_ago]
    remaining_data = df[~df.isin(latest_data)]

    latest_data = latest_data.sort_values(by=["date", "query_date"], ascending=False)
    table_latest = []
    for _, row in latest_data.iterrows():
        table_row = {
            "county": row["county"],
            "department": row["department"],
            "date": row["date"],
            "tag": row["tag"],
            "link": row["link"],
            "title": row["title"],
            "seven_day": row["seven_day"]
        }
        table_latest.append(table_row)

    grouped_remaining = remaining_data.groupby("tag")
    table_remaining = {}
    for tag, group_df in grouped_remaining:
        sort_group_df = group_df.sort_values(
            by=["date", "query_date"], ascending=False)
        table_rows = []
        for _, row in sort_group_df.iterrows():
            table_row = {
                "county": row["county"],
                "department": row["department"],
                "date": row["date"],
                "tag": row["tag"],
                "link": row["link"],
                "title": row["title"],
                "seven_day": row["seven_day"]
            }
            table_rows.append(table_row)
        table_remaining[tag] = table_rows

    return templates.TemplateResponse(
        "main_page.html",
        {"request": request,
         "table_data": table_remaining,
         "latest_data": table_latest,
         "liff_id": liff_id})


@app.get("/ds-manage-assistant-info-web/informed-consent/{liff_id}",
         response_class=HTMLResponse)
async def informed_consent(request: Request, liff_id):
    with open(os.path.join(cwd, "templates", "terms_of_service.md"), "r") as f:
        content = markdown.markdown(f.read())
    return templates.TemplateResponse(
        "informed_consent.html",
        {"request": request,
         "content": content,
         "liff_id": liff_id})


@app.post("/ds-manage-assistant-info-web/log-informed-consent")
async def log_informed_consent(token: UserProfileData, request: Request):
    res = await verify_line_user(token.token, client_id=CHANNEL_ID)
    if res.status_code == 200:
        line_id = res.json().get('sub')
        logger.info({
            "message": f"{line_id} opened payment informed term.",
            "metrics": {"payment_term": 1},
            "labels": {"user_id": f"{line_id}",
                       "status": "payment_term",
                       "timestamp": f"{datetime.utcnow().isoformat()}"}})
    else:
        logger.info(
            f"response.status_code not 200(/log_informed_consent): "
            f"{res.json()}", metrics={'fail': 1})


@app.get("/ds-manage-assistant-info-web/online-payment/{token}/{liff_id}",
         response_class=HTMLResponse)
async def online_payment(request: Request, token, liff_id):
    res = await verify_line_user(token)
    if res.status_code == 200:
        line_id = res.json().get('sub')
    else:
        line_id = "unknown user"
    amount = {
        # 種子liff
        "2001572802-qmpdwDQZ": 1040,
        "2001572802-lQVN4wYn": 936,
        "2001572802-ApYZMzBJ": 728,
        # 金流付款(dev)
        "2001332747-p0dvmQWa": 1040,
        "2001332747-j2A8KpWB": 936,
        "2001332747-w4aEr629": 728,
        # 金流付款(demo)
        "2001389341-mRZWX40Y": 1040,
        "2001389341-LGgzQ9V4": 936,
        "2001389341-bRkmqBvo": 728,
        # 金流付款(release)
        "2001375935-LBkwax1O": 1040,
        "2001375935-ArMlWoLD": 936,
        "2001375935-mEkVXnK4": 728
    }[liff_id]
    html = create_order.create_orders(line_id, amount)
    logger.info({
        "message": f"{line_id} opened ECPay's payment page.",
        "metrics": {"payment_page": 1},
        "labels": {"user_id": line_id,
                   "status": "payment_page",
                   "timestamp": datetime.utcnow().isoformat()}})
    return html


@app.post("/ds-manage-assistant-info-web/finished-payment",
          response_class=HTMLResponse)
async def finished_payment(response: ECPayResponse, request: Request):
    res = response.json()
    if res["Data"]["RtnCode"] == 1:
        title, action = "您已完成付款", "請關閉頁面"
    else:
        title, action = "付款失敗", "請洽詢Jubo客服"

    return templates.TemplateResponse(
        "finished_payment.html",
        {"request": request, "title": title, "action": action}
    )


def main():
    uvicorn.run("linkinpark.app.ds.ma_industryInformation.server:app",
                host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
