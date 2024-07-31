"""
ReportName: 住民人日數
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook

from linkinpark.app.ds.reportPlatformBackend.utils import (ReportGenerateError,
                                                           check_org_type,
                                                           get_nis_data,
                                                           preprocess_date,
                                                           trans_timezone)


def host_amount(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a two sheet report, one for the amount hosted
    everyday in the report period the other one will be detail name list of
    hosted person.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    if not check_org_type(org, "nis"):
        raise ReportGenerateError("此為住宿型機構專屬報表，無法應用於其他機構類型。")

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    collection = ("transfermanages", "patients")
    condition = (
        {"organization": org,
         "createdDate": {"$lt": query_end},
         "status": {"$in": ["newcomer",
                            "hospTransfer",
                            "unplannedHosp",
                            "return",
                            "discharge",
                            "absent",
                            ]
                    }
         },
        {"organization": org, "isDeleted": {"$ne": True}}
    )
    columns = (
        {"_id": 0,
         "patient": 1,
         "status": 1,
         "bedHold": 1,
         "createdDate": 1,
         },
        {"lastName": 1,
         "firstName": 1,
         }
    )
    transfer_df, patient_df = get_nis_data(collection, condition, columns)

    # Preprocess section
    transfer_df.createdDate = trans_timezone(
        transfer_df.createdDate, from_utc=0, to_utc=8)
    count_status = ["newcomer", "return"]
    other_status = ["hospTransfer", "unplannedHosp", "discharge", "absent"]
    transfer_df.loc[transfer_df.status.isin(count_status), "count"] = "yes"
    transfer_df.loc[transfer_df.status.isin(other_status), "count"] = "no"
    transfer_df = transfer_df.sort_values(["patient", "createdDate"])
    transfer_df["endDate"] = (
        transfer_df.groupby(["patient"])["createdDate"].shift(-1)
    )
    transfer_df["key"] = 1
    patient_df["name"] = patient_df.lastName + patient_df.firstName
    date_df = pd.DataFrame(
        pd.Series(
            pd.date_range(start, end - relativedelta(days=1), freq="D")
        ).dt.date, columns=["date"]
    )
    date_df["key"] = 1

    # Merging section
    result_sheet2 = (
        pd.merge(date_df, transfer_df, on="key").drop(labels="key", axis=1))
    result_sheet2 = (
        result_sheet2.merge(patient_df, left_on="patient", right_on="_id"))

    # Computing section
    result_sheet2 = result_sheet2[
        (result_sheet2["createdDate"].dt.date <= result_sheet2["date"])
        & (result_sheet2["count"] == "yes")
        & ((result_sheet2["date"] < result_sheet2["endDate"].dt.date)
           | (pd.isna(result_sheet2["endDate"])))]
    result_sheet1 = result_sheet2.groupby(["date"]).agg({"name": "count"})

    # Formatting section
    result_sheet1 = result_sheet1.reset_index().rename(
        columns={"date": "日期", "name": "人數"})
    keep_columns = ["date", "name", "status", "createdDate"]
    result_sheet2 = result_sheet2[keep_columns].sort_values(["date", "name"])
    result_sheet2 = (
        result_sheet2.replace(
            {"status": {
                "newcomer": "新入住",
                "return": "返院",
                "absent": "請假",
                "hospTransfer": "一般住院",
                "unplannedHosp": "非計畫住院",
            }})
        .rename(
            columns={"date": "日期",
                     "name": "姓名",
                     "status": "案況",
                     "createdDate": "案況日期",
                     }
        )
    )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet1 = workbook.active
    worksheet1.title = "人日數統計"
    worksheet2 = workbook.create_sheet(title="人日數明細")

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result_sheet1.to_excel(writer, sheet_name="人日數統計", index=False)
        result_sheet2.to_excel(writer, sheet_name="人日數明細", index=False)
        worksheet1.column_dimensions["A"].width = 15
        worksheet1.column_dimensions["B"].width = 10
        worksheet2.column_dimensions["A"].width = 15
        worksheet2.column_dimensions["B"].width = 10
        worksheet2.column_dimensions["C"].width = 10
        worksheet2.column_dimensions["D"].width = 15

    return report_file
