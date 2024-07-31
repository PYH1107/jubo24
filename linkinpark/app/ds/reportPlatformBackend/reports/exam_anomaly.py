"""
ReportName: 檢驗異常名單
POC: Shen Chiang
"""

import json
from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook

from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, ReportGenerateError, get_nis_data, preprocess_date,
    trans_timezone)


def exam_anomaly(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a tracking list of patient's abnormal items
    in their exam report.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Regulation of this report
    if suffix is None:
        raise ReportGenerateError(
            "此份報表必須提供額外參數才能正常運作，額外參數應為"
            "{'顯示名稱': '檢驗項目'}。"
            "(eg., {'飯前血糖': 'acs', '飯後血糖': 'pcs'})")

    # Querying section
    danger_dict = json.loads(suffix)
    start, end, query_start, query_end = preprocess_date([start, end])
    patient, exam_record = get_nis_data(
        ["patients", "examreport2"],
        [{"organization": org, "isDeleted": {"$ne": True}},
         {"createdDate": {"$gte": query_start,
                          "$lt": query_end},
          "danger": {"$ne": []}}],
        [{"lastName": 1,
          "firstName": 1,
          "room": 1,
          "bed": 1,
          "patientNumber": 1},
         None]
    )

    # Preprocess Section
    if exam_record.empty:
        raise ReportEmptyError("查詢區間內查無相關檢驗報告紀錄")
    patient["name"] = patient["lastName"] + patient["firstName"]
    patient["ins_id"] = patient["room"] + patient["bed"]

    exam_record["createdDate"] = trans_timezone(
        exam_record["createdDate"].dt.date,
        from_utc=0,
        to_utc=8,
        date_only=True,
    )

    track_list = []
    for p in range(len(exam_record)):
        exam_date = (exam_record.loc[[p]].createdDate
                     .item()
                     .strftime("%Y-%m-%d"))
        content = [exam_date]
        for name, key in danger_dict.items():
            if key in exam_record.loc[[p]].danger.item():
                value = exam_record.at[p, "examData"][key]
                abnormal = (name + ": " + str(value))
                content.append(abnormal)
        if len(content) <= 1:
            content = None
        else:
            content = "\n".join(content)
        track_list.append(content)

    exam_record["track"] = track_list
    exam_record = exam_record[~exam_record.track.isna()]

    # Merging section
    result = pd.merge(
        patient,
        exam_record,
        how="left",
        left_on="_id",
        right_on="patient",
    )

    # Computing section
    # No computing required for this report

    # Formatting section
    keep_column = {
        "ins_id": "機構排序",
        "patientNumber": "文件排序",
        "name": "姓名",
        "track": "需追蹤項目"}
    result = result.sort_values(["ins_id", "patientNumber"])
    result = result[keep_column.keys()].rename(columns=keep_column)

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "檢驗異常名單"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="檢驗異常名單", index=False)
        for column in ["A", "B", "C", "D"]:
            worksheet.column_dimensions[column].width = 15

    return report_file
