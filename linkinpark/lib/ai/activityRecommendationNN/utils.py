import os
import json
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta
from fastapi import Query
from pydantic import BaseModel
from pytz import timezone

from linkinpark.lib.common.logger import CloudLogger
from linkinpark.lib.common.mongo_connector import MongodbReadOnly

labels = {"env": "prod", "app": "Activity recommendation NN"}
LOG = CloudLogger.manager.getPlaceHolder(labels=labels)

file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "activity",
    "activity_categories.json"
)

CAT_DICT = json.load(open(file, "r"))
UNKNOWN = 999


class Content(BaseModel):
    org: str = Query(
        title="Organization ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24, max_length=24
    )
    patient: str = Query(
        title="Patient ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24, max_length=24
    )
    time: datetime = Query(
        title="Prediction time",
        description="The datetime for activity prediction in ISO 8601 format."
    )
    act: str = Query(
        title="Previous activity",
        description="Previous activity name, which has no function in this "
                    "version but kept to remain versatility with older "
                    "version."
    )
    act_details: str = Query(
        title="Previous activity details",
        description="Previous activity details name, which will be used to "
                    "predict the next activity in this version."
    )


class LogInfo(BaseModel):
    _id: str = Query(
        title="Prediction ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24, max_length=24
    )
    selected: str = Query(
        title="Item selected",
        description="The activity name of the item selected",
    )


FORM_ID = "64817780c467420027f85c9fs"
EVENT_SCHEMA = {
    "Q1": "event_start",
    "Q2": "event_end",
    "Q3": "activity_type",
    "Q4": "activity_name"
}
TWZ = timezone("Asia/Taipei")
UTC = timezone("UTC")


def get_data():
    db_conn = MongodbReadOnly(env="prod", database="customizeforms")
    query_date = (UTC.localize(datetime.utcnow()) - relativedelta(
        months=3)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    date_filter = {"data.Q1": {"$gte": query_date}}
    data = pd.DataFrame(db_conn[FORM_ID].find(date_filter))
    return data


def data_preprocess(data):
    df = pd.merge(
        data, data["data"].apply(pd.Series), left_index=True, right_index=True
    ).rename(columns=EVENT_SCHEMA)
    for col in ("event_start", "event_end"):
        df[col] = pd.to_datetime(df[col])
    id_col = ["organization", "patient", "createdUser", "lastUpdatedUser"]
    df = df[id_col + [*EVENT_SCHEMA.values()]]
    for col in id_col:
        df[col] = df[col].astype(str)
    categories = []
    for item in CAT_DICT.values():
        categories += item
    df = df[df["activity_name"].isin(categories)].reset_index()
    return df


def create_features(df):
    df["hour"] = df["event_start"].dt.hour
    df["hour_ge"] = pd.cut(
        df["hour"],
        [-1, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24],
        labels=range(12)
    )
    df["hour_go"] = pd.cut(
        df["hour"],
        [-1, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25],
        labels=list(range(12)) + [0],
        ordered=False)
    df["minute"] = df["event_start"].dt.minute
    df["minute_group"] = pd.cut(
        df["minute"], [-1, 15, 30, 45, 60], labels=list(range(4)))
    df["weekday"] = df["event_start"].dt.weekday
    df["prev_act_name"] = df["activity_name"].shift(1)
    return df


def data_transform(df):
    coding_map = {}
    act_name = list(df["activity_name"].unique())
    for i in range(len(act_name)):
        coding_map[act_name[i]] = i
    for col in ("activity_name", "prev_act_name"):
        df[col] = df[col].replace(coding_map)
    df = df.dropna(subset=["prev_act_name"])
    features = [
        "hour_ge",
        "hour_go",
        "minute_group",
        "weekday",
        "prev_act_name"
    ]
    return df[features], df["activity_name"], coding_map


def prepare_data():
    data = get_data()
    df = data_preprocess(data)
    df = create_features(df)
    result = data_transform(df)
    return result


def gen_features(schema, content):
    content = content.dict()
    act_detail = schema[content["act_details"]] \
        if content["act_details"] in schema else UNKNOWN
    hour = content["time"].hour
    hour_ge = hour // 2
    hour_go = (hour + 1) // 2 % 12
    minute = content["time"].minute
    minute_group = minute // 15
    weekday = content["time"].weekday()
    data = pd.DataFrame(
        [[hour_ge, hour_go, minute_group, weekday, act_detail]],
        columns=[
            "hour_ge", "hour_go", "minute_group", "weekday", "prev_act_name"]
    )
    return data


def get_top_three(probability, schema):
    prob = pd.Series(probability)
    top_3_prob = list(prob.nlargest(3, keep="first").index)
    schema_rev = {v: k for k, v in schema.items()}
    result = []
    for item in top_3_prob:
        result.append(schema_rev[item])
    return result


def get_activity_settings():
    result = {}
    for k, v in CAT_DICT.items():
        for i in v:
            result[i] = k
    return result


def get_category(activities, schema):
    result = []
    for name in activities:
        result.append(schema[name])
    return result
