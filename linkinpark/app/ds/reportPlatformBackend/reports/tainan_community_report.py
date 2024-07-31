"""
ReportName: 台南社區月報
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, Side

from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportGenerateError, get_nis_data, preprocess_date)


def tainan_community_report(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic report of service provided for
    Tainan.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Regulation of this report
    if org != [
        ObjectId("5fa11ea03911890c6d7f6411"),
        ObjectId("5fa133c73911890c6d7f684e"),
        ObjectId("5fa128203911890c6d7f6414")
    ]:
        raise ReportGenerateError("此為萃文集團專屬報表，無法應用於其他機構。")

    # Querying section
    _ = suffix
    times = preprocess_date([datetime(start.year, 1, 1), start, end])
    year, start, end, query_year, query_start, query_end = times
    patient, attendance, users = get_nis_data(
        ["patients", "users"],
        [{"organization": {"$in": org}, "isDeleted": {"$ne": True}},
         {"organization": {"$in": org},
          "staffStatus": "employed",
          "isDeleted": {"$ne": True},
          "jobType": {"$in": ["foreign-nurse-aide", "native-nurse-aide"]}}],
        [{"sex": 1,
          "organization": 1},
         {"organization": 1,
          "sex": 1}])
    patient_list = patient["_id"].to_list()
    attendance = get_nis_data(
        "patientattendances",
        {"organization": {"$in": org},
         "patient": {"$in": patient_list},
         "presentStatus": "present",
         "$and": [{"checkInTime": {"$gte": query_year}},
                  {"checkInTime": {"$lt": query_end}}]},
        {"_id": 0,
         "checkInTime": 1,
         "patient": 1,
         "organization": 1})

    org_date = pd.DataFrame(
        [[0,
          ObjectId("5fa11ea03911890c6d7f6411"),
          "臺南市政府社會局委託財團法人高雄市私立萃文書院社會福利"
          "慈善事業基金會經營臺南市官田老人養護中心"],
         [1,
          ObjectId("5fa133c73911890c6d7f684e"),
          "財團法人高雄市私立萃文書院社會福利慈善事業基金會附設臺"
          "南市私立萃文龍崎社區式服務類長期照顧服務機構"],
         [2,
          ObjectId("5fa128203911890c6d7f6414"),
          "財團法人高雄市私立萃文書院社會福利慈善事業基金會附設臺"
          "南市私立萃文南化社區式服務類長期照顧服務機構"]],
        columns=["order", "org_id", "org_name"])

    # Preprocess Section
    if len(attendance) == 0:
        raise ValueError("No record found during the report period.")
    year_amount = (attendance.groupby("organization").agg(
        year_total=("patient", "nunique")))
    users_amount = (users.groupby(["organization", "sex"]).agg(
        users=("_id", "nunique")).reset_index())
    users_amount = users_amount.pivot(
        index="organization",
        columns="sex",
        values="users",
    )
    org_df = pd.DataFrame(org, columns=["organization"])
    org_df["key"] = 1
    sex_df = pd.DataFrame(["male", "female"], columns=["sex"])
    sex_df["key"] = 1
    org_df = pd.merge(org_df, sex_df, on="key").drop(columns="key")
    month_attend = attendance[attendance["checkInTime"] >= query_start].copy()

    month_attend = pd.merge(
        month_attend,
        patient,
        how="left",
        left_on=["patient", "organization"],
        right_on=["_id", "organization"]
    )
    month_attend = pd.merge(
        org_df,
        month_attend,
        how="left",
        left_on=["organization", "sex"],
        right_on=["organization", "sex"]
    )
    total_attennd = (
        month_attend.groupby("organization").agg(
            total_amount=("patient", "nunique"),
            total_freq=("patient", "count")))
    gender_attend = (
        month_attend.groupby(["organization", "sex"]).agg(
            gender_amount=("patient", "nunique"),
            gender_freq=("patient", "count")).reset_index())
    gender_attend = gender_attend.pivot(
        index="organization",
        columns="sex",
        values=["gender_amount",
                "gender_freq"]
    )
    gender_attend.columns = gender_attend.columns.to_flat_index()
    gender_attend.columns = [
        "female_amount",
        "male_amount",
        "female_freq",
        "male_freq",
    ]

    # Merging section
    result = pd.merge(
        total_attennd,
        gender_attend,
        left_index=True,
        right_index=True
    )
    result = pd.merge(result, users_amount, left_index=True, right_index=True)
    result = pd.merge(result, year_amount, left_index=True, right_index=True)
    result = pd.merge(result, org_date, left_index=True, right_on="org_id")

    # Computing section
    result = result.sort_values("order")
    keep_columns = [
        "org_name",
        "total_amount",
        "total_freq",
        "male_amount",
        "male_freq",
        "female_amount",
        "female_freq",
        "male",
        "female",
        "year_total",
    ]
    result = result[keep_columns]
    result = result.fillna(0)
    result = result.rename(
        columns={"org_name": "單位名稱",
                 "total_amount": "當月總服務人數",
                 "total_freq": "當月總服務人次",
                 "male_amount": "當月服務人數(男)",
                 "male_freq": "當月服務人次(男)",
                 "female_amount": "當月服務人數(女)",
                 "female_freq": "當月服務人次(女)",
                 "male": "當月男照服員人數",
                 "female": "當月女照服員人數",
                 "year_total": "歸戶人數"}
    )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "社區式日間照顧成果報表"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="社區式日間照顧成果報表",
                        index=False)
        columns_to_format = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
        for column in columns_to_format:
            worksheet.column_dimensions[column].width = 10.0
        worksheet.column_dimensions["A"].width = 50.0

        thin = Side(border_style="thin", color="000000")
        for col in columns_to_format:
            for row in range(1, 7):
                location = col + str(row)
                cell = worksheet[location]
                cell.alignment = Alignment(wrap_text=True)
                cell.border = Border(
                    left=thin,
                    right=thin,
                    top=thin,
                    bottom=thin)
                cell.font = Font(bold=False)

        for col in columns_to_format:
            cell = col + "6"
            if cell == "A6":
                worksheet[cell] = "當月合計"
            else:
                worksheet[cell] = f"=SUM({col}2:{col}4)"

    return report_file
