from datetime import datetime

import pandas as pd
from fastapi import Query
from pydantic import BaseModel
from pytz import timezone

from linkinpark.lib.common.mongo_connector import MongodbReadOnly

UTC = timezone("UTC")


class ActContent(BaseModel):
    org: str = Query(
        title="Organization ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24,
    )
    patient: str = Query(
        title="Patient ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24,
    )
    activity: str = Query(
        title="The activity category",
        description="The category of just ended activity.",
    )
    start: datetime = Query(
        title="Period start time",
        description="The datetime in ISO 8601 format."
    )
    end: datetime = Query(
        title="Period end time", description="The datetime in ISO 8601 format."
    )


class BevContent(BaseModel):
    org: str = Query(
        title="Organization ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24,
    )
    patient: str = Query(
        title="Patient ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24,
    )
    status: str = Query(
        title="The behaviour status",
        description="The status should be either awake or asleep.",
    )
    start: datetime = Query(
        title="Period start time",
        description="The datetime in ISO 8601 format."
    )
    end: datetime = Query(
        title="Period end time", description="The datetime in ISO 8601 format."
    )


MENTAL_ID = "6481770a2ed7b6002788ebdfs"
MENTAL_SCHEMA = {
    "Q1": "start_time",
    "Q2": "mental_score",
    "Q3": "end_time",
}


def get_data(start, end):
    db_conn = MongodbReadOnly(env="prod", database="customizeforms")
    time_format = "%Y-%m-%dT%H:%M:%S.000Z"
    query_start, query_end = start.strftime(time_format), end.strftime(
        time_format)
    date_filter = {"data.Q1": {"$lte": query_end},
                   "data.Q3": {"$gte": query_start}}
    data = pd.DataFrame(db_conn[MENTAL_ID].find(date_filter))
    return data


def data_preprocess(data, start, end):
    df = pd.merge(
        data, data["data"].apply(pd.Series), left_index=True, right_index=True
    ).rename(columns=MENTAL_SCHEMA)
    for col in ("start_time", "end_time"):
        df[col] = pd.to_datetime(df[col])
    df.loc[df["start_time"] < start, "start_time"] = start
    df.loc[df["end_time"] > end, "end_time"] = end
    id_col = ["organization", "patient"]
    df = df[id_col + [*MENTAL_SCHEMA.values()]]
    for col in id_col:
        df[col] = df[col].astype(str)
    df["duration"] = (df["end_time"] - df["start_time"]).dt.seconds
    df["mental_score"] = df["mental_score"].astype(int)
    df["weight_score"] = df["mental_score"] * df["duration"]
    return df


def gen_features(start, end):
    data = get_data(start, end)
    df = data_preprocess(data, start, end)
    df["status"] = pd.cut(
        df["mental_score"],
        bins=[-1, 4, 6, 10],
        labels=["asleep", "awake", "active"]
    )
    df["pre_status"] = df["status"].shift()
    df.loc[
        (df["pre_status"] == "asleep") & (df["status"] != "asleep"), "wake_up"
    ] = True
    df.loc[
        (df["pre_status"] != "asleep")
        & (df["status"] == "asleep"), "fall_asleep"
    ] = True
    result = {"score_avg": df["weight_score"].sum() / df["duration"].sum(),
              "asleep_avg": (
                  df.loc[df["status"] == "asleep", "duration"].sum()
                  / df["duration"].sum()),
              "awake_avg": (
                  df.loc[df["status"] == "awake", "duration"].sum()
                  / df["duration"].sum()),
              "active_avg": (
                  df.loc[df["status"] == "active", "duration"].sum()
                  / df["duration"].sum()),
              "wake_up_avg": df["wake_up"].sum(),
              "fall_asleep_avg": df["fall_asleep"].sum()}
    return result
