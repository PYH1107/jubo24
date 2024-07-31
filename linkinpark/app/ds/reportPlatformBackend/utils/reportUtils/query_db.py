from datetime import date, datetime

import pandas as pd
from bson import ObjectId
from linkinpark.app.ds.reportPlatformBackend.utils.reportUtils.time_transformer \
    import trans_timezone

from linkinpark.lib.common.mongo_connector import MongodbReadOnly


def get_nis_data(collections: (str, tuple, list),
                 conditions: (tuple, list, dict) = None,
                 columns: (tuple, list, dict) = None
                 ) -> (list, pd.DataFrame):
    """
    This function will extract data stored inside the NIS mongoDB based on
    the parameters provided to the function.
    :param collections: The collection to extract data.
    :param conditions: The condition while querying.
    :param columns: The columns to extract.
    :return: The querying result.
    """

    db = MongodbReadOnly("prod")

    if isinstance(collections, str):
        result = pd.DataFrame(db[collections]
                              .find(conditions, columns))
    elif hasattr(collections, "__iter__"):
        result = []
        for index, collection in enumerate(collections):
            data = pd.DataFrame(db[collection]
                                .find(conditions[index], columns[index]))
            result.append(data)
    else:
        raise AttributeError(f"collections' attribute must not be "
                             f"{type(collections)}.")

    return result


def clients_infile(start: (datetime, date),
                   end: (datetime, date),
                   organization: (tuple, list, ObjectId),
                   still_open: bool = False) -> pd.DataFrame:
    start, end = trans_timezone((start, end), from_utc=8, to_utc=0)
    if not hasattr(organization, "__iter__"):
        organization = [organization]
    df = get_nis_data(
        "transfermanages",
        {
            "status": {
                "$in": [
                    "startServer",
                    "newcomer",
                    "closed",
                    "pause",
                    "continue",
                    "discharge",
                ]},
            "createdDate": {"$lt": end},
            "organization": {"$in": organization}},
        {
            "_id": 0,
            "createdDate": 1,
            "firstServerDate": 1,
            "organization": 1,
            "patient": 1,
            "status": 1
        }
    )
    if "firstServerDate" in df.columns:
        df.loc[
            ~pd.isna(df["firstServerDate"]), "createdDate"
        ] = df["firstServerDate"]
    df.sort_values(["patient", "createdDate"], inplace=True)
    df["pre_status"] = df.groupby("patient")["status"].shift(1)
    df["next_status"] = df.groupby("patient")["status"].shift(-1)
    df.drop(df[df["status"] == df["pre_status"]].index, inplace=True)
    df["next_status_date"] = df.groupby("patient")["createdDate"].shift(-1)
    df = df[df["status"].isin(["startServer", "newcomer", "continue"])]
    df.rename(columns={"createdDate": "open_at",
                       "next_status_date": "close_at"},
              inplace=True)
    df = df.loc[(df.open_at <= end)
                & ((df.close_at >= start) | (df.close_at.isna()))]
    if still_open:
        df = df[((df.close_at >= end) | (df.close_at.isna()))]

    date_col = ["open_at", "close_at"]
    for col in date_col:
        df[col] = trans_timezone(df[col], 0, 8, ignore_nan=True)
    keep = ["organization", "patient", "open_at", "close_at"]

    return df[keep]


def check_org_type(org: ObjectId, org_type) -> bool:
    db = MongodbReadOnly("prod")
    res = db["organizations"].find_one(
        {"_id": org}, {"solution": 1, "subSolution": 1})
    if "subSolution" not in res:
        res["subSolution"] = None
    if res["solution"] == org_type or res["subSolution"] == org_type:
        return True
    else:
        return False
