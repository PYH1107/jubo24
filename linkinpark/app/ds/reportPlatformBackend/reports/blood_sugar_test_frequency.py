"""
ReportName: 血糖紀錄
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook
from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, get_nis_data, preprocess_date)


def blood_sugar_test_frequency(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic report of how many times of blood
     sugar measurement was done for each patient during the report period.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    bloodsugar_df, patient = get_nis_data(
        ["bloodsugars", "patients"],
        [{"organization": org,
          "createdDate": {"$gte": query_start,
                          "$lt": query_end}},
         {"organization": org, "isDeleted": {"$ne": True}}],
        [{"_id": 0,
          "patient": 1,
          "createdDate": 1},
         {"lastName": 1,
          "firstName": 1}])

    # Preprocess Section
    if bloodsugar_df.empty:
        raise ReportEmptyError("查詢區間內查無相關血糖紀錄。")
    patient["name"] = patient["lastName"] + patient["firstName"]

    # Merging section
    result = pd.merge(
        bloodsugar_df, patient, left_on="patient", right_on="_id")

    # Computing section
    result = result.groupby("name").size().reset_index()

    # Formatting section
    result.rename(columns={"name": "姓名", 0: "血糖紀錄次數"}, inplace=True)

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "血糖紀錄"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="血糖紀錄", index=False)
        for column in ["A", "B"]:
            worksheet.column_dimensions[column].width = 15

    return report_file
