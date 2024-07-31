"""
ReportName: 換管名冊
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


def change_tubes(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a list of clients who should change tubes in
    the report period.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    collections = ("tuberecords", "tubes", "patients", "transfermanages")
    conditions = ({"organization": org,
                   "nextReplaceDate": {"$lt": query_end,
                                       "$gte": query_start}},
                  {"organization": org},
                  {"organization": org, "isDeleted": {"$ne": True}},
                  # Condition for createdDate was added due to dirty data in
                  # test organization which will lead to failure on unit test.
                  {"organization": org,
                   "createdDate": {"$lt": datetime(3000, 1, 1)}})
    columns = ({"_id": 0,
                "createdDate": 1,
                "material": 1,
                "size": 1,
                "sizeUnit": 1,
                "statement": 1,
                "nextReplaceDate": 1,
                "patient": 1,
                "tubeId": 1,
                },
               {"finished": 1,
                "type": 1,
                },
               {"lastName": 1,
                "firstName": 1,
                },
               {"_id": 0,
                "room": 1,
                "bed": 1,
                "createdDate": 1,
                "patient": 1,
                "status": 1,
                }
               )
    record_df, tube_df, patient_df, transfer_df = get_nis_data(
        collections, conditions, columns
    )

    # Preprocess section
    if record_df.empty:
        raise ReportEmptyError("查詢區間內查無相關管路紀錄。")
    transfer_df = transfer_df.loc[
        transfer_df.groupby("patient").createdDate.idxmax()
    ]

    # Merging section
    result = pd.merge(
        record_df, tube_df, "left", left_on="tubeId", right_on="_id")
    result = pd.merge(
        result, patient_df, left_on="patient", right_on="_id")
    result = pd.merge(
        result, transfer_df, "left", on="patient")

    # Computing section
    result = result[~result["status"].isin(["discharge", "closed"])]

    # Formatting section
    date_columns = ("createdDate_x", "nextReplaceDate")
    for col in date_columns:
        result[col] = trans_timezone(
            result[col],
            from_utc=0,
            to_utc=8,
            date_only=True,
        )
    result["name"] = result["lastName"] + result["firstName"]
    result["bed_no"] = result["room"] + "-" + result["bed"]
    result = result[~result["finished"]]
    keep_column = [
        "bed_no",
        "name",
        "type",
        "size",
        "sizeUnit",
        "material",
        "createdDate_x",
        "nextReplaceDate",
    ]
    result = result[keep_column]
    result = result.sort_values(["bed_no", "nextReplaceDate"])
    result = result.replace(
        {"type": dic.tube_type,
         "material": dic.tube_material,
         }
    )
    result = result.rename(
        columns={"bed_no": "床號",
                 "name": "住民",
                 "type": "插管類型",
                 "size": "尺寸",
                 "sizeUnit": "單位",
                 "material": "材質",
                 "createdDate_x": "上次換管日期",
                 "nextReplaceDate": "下次換管日期",
                 }
    )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "換管名冊"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="換管名冊", index=False)
        for column in ["B", "C", "G", "H"]:
            worksheet.column_dimensions[column].width = 15

    return report_file
