"""
ReportName: 導尿管人日數統計
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
from bson.objectid import ObjectId
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook

from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, get_nis_data, preprocess_date)


def urinary_tube_day_count(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a list of of each day how many person has use
    the urinary tube.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    _ = suffix
    # Querying section
    start, end, query_start, query_end = preprocess_date([start, end])
    collection = ["transfermanages", "patients", "tubes"]
    condition = [
        {"organization": org,
         "createdDate": {"$lt": query_end},
         "status": {"$in": ["newcomer", "hospTransfer",
                            "unplannedHosp", "return",
                            "discharge", "absent", "bedTransfer",
                            "branchTransfer", "unpresented"]}},
        {"organization": org, "isDeleted": {"$ne": True}},
        {"organization": org,
         "createdDate": {"$lt": query_end},
         "$or": [{"finishedDate": None},
                 {"finishedDate": {"$gte": query_start}}],
         "type": "foley"},
    ]
    columns = [
        {"_id": 0,
         "patient": 1,
         "status": 1,
         "bedHold": 1,
         "createdDate": 1},
        {"lastName": 1,
         "firstName": 1,
         "branch": 1,
         "bed": 1,
         "room": 1},
        {"_id": 0,
         "createdDate": 1,
         "type": 1,
         "organization": 1,
         "patient": 1,
         "finished": 1,
         "finishedDate": 1,
         "toQI": 1},
    ]
    trans_df, patient_df, tube_record = get_nis_data(
        collection, condition, columns
    )

    if tube_record.empty:
        raise ReportEmptyError("查詢區間內查無相關管路紀錄。")

    # Preprocess section
    date_col = ["createdDate", "createdDate", "finishedDate"]
    target_df = [trans_df] + [tube_record] * 2
    for df in (trans_df, tube_record):
        df["createdDate"] += pd.Timedelta(hours=8)
    if "finishedDate" in tube_record.columns:
        tube_record["finishedDate"] = tube_record["finishedDate"].replace({
            np.nan: None})
        tube_record["finishedDate"] += pd.Timedelta(hours=8)
    else:
        tube_record["finishedDate"] = None

    count_status = [
        "newcomer", "return", "bedTransfer", "branchTransfer",
    ]
    other_status = [
        "hospTransfer", "unplannedHosp", "discharge", "absent", "unpresented",
    ]
    trans_df.loc[trans_df.status.isin(count_status), "count"] = "yes"
    trans_df.loc[trans_df.status.isin(other_status), "count"] = "no"
    trans_df.sort_values(["patient", "createdDate"], inplace=True)
    shift_col = {"createdDate": "endDate", "status": "pre_status"}
    for old_col, new_col in shift_col.items():
        trans_df[new_col] = trans_df.groupby("patient")[old_col].shift(-1)
    trans_df = trans_df[
        ~((trans_df["status"] == "unpresented")
          & pd.isna(trans_df["pre_status"]))
    ].copy()
    trans_df["key"], trans_df["hosted"] = 1, False
    patient_df["name"] = patient_df.lastName + patient_df.firstName
    patient_df.rename(columns={"_id": "patient"}, inplace=True)
    date_df = pd.DataFrame(
        pd.Series(
            pd.date_range(start, end - relativedelta(days=1), freq="D")
        ).dt.date,
        columns=["date"]
    )
    date_df["key"] = 1
    tube_record.rename(columns={"createdDate": "startDate"}, inplace=True)

    # Merging section
    host_record = pd.merge(date_df, trans_df, on="key").drop(columns="key")
    host_record = host_record.merge(patient_df, on="patient")

    # Computing section
    host_record.loc[
        (host_record["createdDate"].dt.date <= host_record["date"])
        & (host_record["count"] == "yes")
        & ((host_record["date"] < host_record["endDate"].dt.date)
           | (pd.isna(host_record["endDate"]))),
        "hosted"
    ] = True
    tube_record = host_record.merge(tube_record, "left", on="patient")
    tube_record = tube_record[tube_record["hosted"]]
    tube_record["with_tube"] = False
    tube_record.loc[
        (tube_record["date"] >= tube_record["startDate"].dt.date)
        & ((tube_record["date"] < tube_record["finishedDate"].dt.date)
           | (pd.isna(tube_record["finishedDate"]))),
        "with_tube"] = True
    result = tube_record.groupby(["date", "branch"]).agg(
        {"patient": "nunique", "with_tube": "sum"}
    )
    result["without_tube"] = result["patient"] - result["with_tube"]
    result = result.groupby("branch").agg("sum").reset_index()
    summary = pd.DataFrame(result.sum()).T
    summary["branch"] = "加總"
    result = pd.concat([result, summary])

    # Formatting section
    result.sort_values(["branch"], inplace=True)
    result.rename(columns={
        "branch": "區域",
        "patient": "在院人數",
        "with_tube": "使用導尿管人數",
        "without_tube": "未使用導尿管人數"
    }, inplace=True)

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet1 = workbook.active
    worksheet1.title = "每月導尿管人日數"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="每月導尿管人日數", index=False)
        for sheet, columns in ((worksheet1, ("A", "C", "D", "E")),):
            for column in columns:
                sheet.column_dimensions[column].width = 20

    return report_file
