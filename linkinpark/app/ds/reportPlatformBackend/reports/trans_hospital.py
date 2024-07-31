"""
ReportName: 住民異動
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook

from linkinpark.app.ds.reportPlatformBackend.utils import (
    get_nis_data, preprocess_date, trans_timezone)


def trans_hospital(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic report of clients hospital status.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    start, end = pd.to_datetime(start), pd.to_datetime(end)
    patient_df = get_nis_data(
        "patients",
        {"organization": org, "isDeleted": {"$ne": True}},
        {"lastName": 1, "firstName": 1})
    patient_list = patient_df["_id"].to_list()
    transfer_df = get_nis_data(
        "transfermanages",
        {"organization": org,
         "patient": {"$in": patient_list},
         "createdDate": {"$lt": query_end},
         "status": {"$in": ["newcomer",
                            "hospTransfer",
                            "unplannedHosp",
                            "return",
                            "discharge",
                            "absent"]}},
        {"createdDate": 1,
         "patient": 1,
         "status": 1,
         "branch": 1,
         "room": 1,
         "bed": 1,
         "reason": 1}
    )

    # Preprocess section
    if len(transfer_df) == 0:
        result = pd.DataFrame(
            columns=("分院",
                     "房號",
                     "床號",
                     "住民",
                     "狀態",
                     "住院日期",
                     "是否出院",
                     "出院日期",
                     "住院天數",
                     )
        )
        report_file = BytesIO()
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "住民異動"

        with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
            writer.book = workbook
            writer.sheets = \
                dict((ws.title, ws) for ws in workbook.worksheets)
            result.to_excel(writer, sheet_name="住民異動", index=False)
            worksheet.column_dimensions["D"].width = 10
            worksheet.column_dimensions["E"].width = 15
            worksheet.column_dimensions["F"].width = 15
            worksheet.column_dimensions["H"].width = 15

        return report_file

    transfer_df["createdDate"] = trans_timezone(
        transfer_df.createdDate, from_utc=0, to_utc=8)
    transfer_df = transfer_df.sort_values(["patient", "createdDate"])
    transfer_df["end_record"] = (
        transfer_df.groupby(["patient"])["_id"].shift(-1)
    )
    patient_df["name"] = patient_df["lastName"] + patient_df["firstName"]

    # Merging section
    result = pd.merge(transfer_df,
                      transfer_df,
                      how="left",
                      left_on="end_record",
                      right_on="_id",
                      )
    result = pd.merge(result,
                      patient_df,
                      how="left",
                      left_on="patient_x",
                      right_on="_id",
                      )

    # Computing section
    # drop status unrelated to hospital, and remove those who already
    # discharge from institution.
    start_dt = datetime.combine(start, datetime.min.time())
    result = result.loc[
        (result["status_x"].isin(["unplannedHosp", "hospTransfer"]))
        & ((result["createdDate_y"] >= start_dt)
           | (pd.isna(result["createdDate_y"])))].reset_index(drop=True)

    # reset the branch, room and bed to the latest status.
    result.loc[pd.isna(result["branch_y"]), "branch_y"] = result["branch_x"]
    result.loc[pd.isna(result["room_y"]), "room_y"] = result["room_x"]
    result.loc[pd.isna(result["bed_y"]), "bed_y"] = result["bed_x"]

    # check if the patient has returned to the institution or not.
    result.loc[pd.isna(result["status_y"]), "return"] = "No"
    result.loc[result["status_y"] == "return", "return"] = "Yes"
    result.loc[
        result["status_y"] == "discharge", "return"] = result["reason_y"]

    # count how many days of this month the patient is in hospital.
    result.loc[
        result["createdDate_x"] >= start_dt, "start"] = result["createdDate_x"]
    result["start"] = result["start"].fillna(start).dt.date
    result["end"] = result["createdDate_y"].fillna(end).dt.date
    result["days"] = result["end"] - result["start"]

    # Formatting section
    result["createdDate_x"] = result["createdDate_x"].dt.date
    result["createdDate_y"] = result["createdDate_y"].dt.date
    result = result.sort_values(["branch_y", "room_y", "bed_y"])
    keep_columns = [
        "branch_y",
        "room_y",
        "bed_y",
        "name",
        "status_x",
        "createdDate_x",
        "return",
        "createdDate_y",
        "days",
    ]
    result = (
        result[keep_columns].replace(
            {"status_x": {"unplannedHosp": "非計畫住院",
                          "hospTransfer": "一般住院"},
             "return": {"died": "離世",
                        "hospitalized": "住院",
                        "homeCare": "回家照顧",
                        "trans2other": "轉其他機構",
                        }
             }
        ).rename(
            columns={"branch_y": "分院",
                     "room_y": "房號",
                     "bed_y": "床號",
                     "name": "住民",
                     "status_x": "狀態",
                     "createdDate_x": "住院日期",
                     "return": "是否出院",
                     "createdDate_y": "出院日期",
                     "days": "住院天數",
                     }
        )
    )

    # Filing section
    file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "住民異動"

    with pd.ExcelWriter(file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="住民異動", index=False)
        worksheet.column_dimensions["D"].width = 10
        worksheet.column_dimensions["E"].width = 15
        worksheet.column_dimensions["F"].width = 15
        worksheet.column_dimensions["H"].width = 15

    return file
