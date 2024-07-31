import pandas as pd

from linkinpark.lib.common.mongo_connector import MongodbReadOnly


def get_org_name(org_id):
    conn = MongodbReadOnly("prod", app_name="aids_report_platform")
    org_name = conn["organizations"].find_one({"_id": org_id})["name"]
    return org_name


def search_org_by_nickname(nickname):
    name = nickname.dict()["nickname"]
    conn = MongodbReadOnly("prod", app_name="aids_report_platform")
    res = pd.DataFrame(conn["organizations"].find(
        {"nickName": {"$regex": name}}, {"_id": 1, "name": 1}))
    result = dict(zip(res["name"], res["_id"].astype(str)))
    return result


def search_id_by_name(name):
    name = name.dict()["name"]
    conn = MongodbReadOnly("prod", app_name="aids_report_platform")
    res = conn["organizations"].find_one(
        {"name": name}, {"_id": 1, "nickName": 1})
    res["_id"] = str(res["_id"])
    return res
