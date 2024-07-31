"""
ReportName: IADL總表
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook
from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, get_nis_data, preprocess_date)


def iadl_evaluation_statistic(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic of patients' IADL evaluation score.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    iadl_df, patient_df = get_nis_data(
        ["iadlassessments", "patients"],
        [{"organization": org,
          "updatedDate": {"$gte": query_start, "$lt": query_end}},
         {"organization": org, "isDeleted": {"$ne": True}}],
        [{"_id": 0, "patient": 1, "createdDate": 1, "score": 1},
         {"lastName": 1, "firstName": 1, "branch": 1, "roombed": 1}]
    )

    # Preprocess Section
    if iadl_df.empty:
        raise ReportEmptyError("查詢區間內查無相關日常功能性評估紀錄。")
    patient_df["display_name"] = (
        patient_df["branch"].fillna("")
        + "-"
        + patient_df["roombed"].fillna("")
        + " "
        + patient_df["lastName"].fillna("")
        + patient_df["firstName"].fillna("")
    )
    patient_df["branch"] = patient_df["branch"].astype(int)
    patient_df.rename(columns={"_id": "patient"}, inplace=True)

    # Merging section
    result = pd.merge(iadl_df, patient_df, on="patient")

    # Computing section
    if "score" not in result.columns:
        result["score"] = None
    score_range = ((0, 10), (11, 14), (15, 19), (20, 23), (24, 24))
    col_names = []
    for lower, upper in score_range:
        col_name = str(lower) + "-" + str(upper)
        col_names.append(col_name)
        result.loc[
            (result["score"] >= lower) & (result["score"] <= upper), col_name
        ] = 1
    summary = result[col_names].sum()
    summary["display_name"] = "總計"

    # Formatting section
    result = result.sort_values(["branch", "roombed", "createdDate"])
    result = pd.concat([result, pd.DataFrame([summary])])
    result = result[
        ["display_name", "createdDate", "score"] + col_names]
    result["createdDate"] = (result["createdDate"] + pd.Timedelta(
        hours=8)).dt.strftime("%Y/%m/%d")
    result = result.rename(columns={
        "display_name": "住民姓名", "createdDate": "施測日期",
        "score": "IADL分數"})

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "IADL統計"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="IADL統計", index=False)
        for column, width in {"A": 20, "B": 15, "C": 12}.items():
            worksheet.column_dimensions[column].width = width

    return report_file
