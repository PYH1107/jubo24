"""
ReportName: 移除管路名冊
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook
from linkinpark.app.ds.reportPlatformBackend.utils import schema as dic
from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, get_nis_data, preprocess_date, trans_timezone)


def remove_tubes(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a report of clients' who has removed
    their tubes during period.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])

    tubes = get_nis_data(
        "tubes",
        {"organization": org,
         "finishedDate": {"$lt": query_end,
                          "$gte": query_start}},
        {"createdDate": 1,
         "type": 1,
         "patient": 1,
         "finishedDate": 1,
         "finishedReason": 1,
         }
    )
    if tubes.empty:
        raise ReportEmptyError("查詢區間內查無相關管路紀錄。")

    tube_list = tubes["_id"].tolist()
    patient_list = tubes["patient"].tolist()
    tube_record, patient = get_nis_data(
        ("tuberecords", "patients"),
        ({"organization": org,
          "tubeId": {"$in": tube_list},
          },
         {"organization": org,
          "_id": {"$in": patient_list},
          "isDeleted": {"$ne": True}
          }
         ),
        ({"tubeId": 1,
          "material": 1,
          "size": 1,
          "sizeUnit": 1,
          },
         {"branch": 1,
          "room": 1,
          "bed": 1,
          "lastName": 1,
          "firstName": 1,
          }
         )
    )
    if tube_record.empty:
        raise ReportEmptyError("查詢區間內查無相關管路紀錄。")

    # Preprocess section
    time_columns = ("createdDate", "finishedDate")
    for column in time_columns:
        tubes[column] = trans_timezone(
            tubes[column],
            from_utc=0,
            to_utc=8,
        )

    # Merging section
    result = pd.merge(
        tubes,
        tube_record,
        "left",
        left_on="_id",
        right_on="tubeId",
    )
    result = pd.merge(
        result,
        patient,
        "left",
        left_on="patient",
        right_on="_id",
    )

    # Computing section
    result = result.loc[result.groupby("tubeId").createdDate.idxmin()]

    # Formatting section
    result["name"] = result["lastName"] + result["firstName"]
    result["PID"] = result["branch"] + "-" + result["room"] + result["bed"]

    for column in time_columns:
        result[column] = result[column].dt.date

    result = result[[
        "PID",
        "name",
        "type",
        "size",
        "sizeUnit",
        "material",
        "createdDate",
        "finishedDate",
        "finishedReason",
    ]]

    result = result.replace(
        {"type": dic.tube_type,
         "material": dic.tube_material,
         "finishedReason": dic.tube_remove_reason}
    )
    result = result.rename(
        columns={"PID": "床號",
                 "name": "住民",
                 "type": "插管類型",
                 "size": "尺寸",
                 "sizeUnit": "單位",
                 "material": "材質",
                 "createdDate": "插管日期",
                 "finishedDate": "移除日期",
                 "finishedReason": "移除原因"}
    )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "移除管路名冊"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="移除管路名冊", index=False)
        for column in ["B", "C", "G", "H", "I"]:
            worksheet.column_dimensions[column].width = 15

    return report_file
