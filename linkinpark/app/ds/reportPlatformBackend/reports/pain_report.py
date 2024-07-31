"""
ReportName: 疼痛列表
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook

from linkinpark.app.ds.reportPlatformBackend.utils import (ReportEmptyError,
                                                           get_nis_data,
                                                           preprocess_date)


def pain_report(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a list of pain score of each patients.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    vital_sign, patient = get_nis_data(
        ["vitalsigns", "patients"],
        [{"organization": org,
          "createdDate": {"$gte": query_start,
                          "$lt": query_end}},
         {"organization": org, "isDeleted": {"$ne": True}}],
        [{"_id": 0,
          "createdDate": 1,
          "patient": 1,
          "PAIN": 1},
         {"bed": 1,
          "room": 1,
          "firstName": 1,
          "lastName": 1}])

    # Preprocess Section
    if vital_sign.empty:
        raise ReportEmptyError("查詢區間內查無相關VitalSign紀錄。")

    vital_sign["date"] = vital_sign["createdDate"].dt.date
    vital_sign["time"] = vital_sign["createdDate"].dt.strftime("%H:%M")
    patient["room-bed"] = (
        patient["room"].fillna("") + "-" + patient["bed"].fillna(""))
    patient["name"] = (
        patient["lastName"].fillna("") + patient["firstName"].fillna(""))

    # Merging section
    result = pd.merge(
        vital_sign, patient, left_on="patient", right_on="_id"
    )

    # Computing section
    keep_column = ["date", "time", "room-bed", "name", "PAIN"]
    for col in keep_column:
        if col not in result.columns:
            result[col] = None
    result = (result[keep_column].sort_values(["date", "time", "room-bed"])
              .rename(columns={"date": "日期",
                               "time": "時間",
                               "room-bed": "房號",
                               "name": "姓名",
                               "PAIN": "疼痛分數",
                               }
                      )
              )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "疼痛列表"
    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="疼痛列表", index=False)
        sheet_col_width = {
            "A": 15, "B": 15, "C": 15, "D": 15, "E": 15
        }
        for col in sheet_col_width:
            worksheet.column_dimensions[col].width = sheet_col_width[col]

    return report_file
