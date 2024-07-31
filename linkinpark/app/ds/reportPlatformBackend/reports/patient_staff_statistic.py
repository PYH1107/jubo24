"""
ReportName: 基本資料
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO
from statistics import mean

import pandas as pd
from bson.objectid import ObjectId
from openpyxl import Workbook, styles

from linkinpark.app.ds.reportPlatformBackend.utils import (ReportEmptyError,
                                                           ReportGenerateError,
                                                           clients_infile,
                                                           count_age,
                                                           get_nis_data,
                                                           trans_timezone)


def patient_staff_statistic(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic report of patient and staffs
    background.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Querying section
    _ = suffix
    query_start, query_end = trans_timezone([start, end], from_utc=8, to_utc=0)
    in_file_client = clients_infile(start, end, org, still_open=True)
    clients_list = list(in_file_client["patient"])
    patient, staff, tube = get_nis_data(
        ["patients", "users", "tubes"],
        [{"organization": org,
          "_id": {"$in": clients_list},
          "isDeleted": {"$ne": True}},
         {"organization": org,
          "employDate": {"$lt": query_end},
          "$or": [{"resignDate": None},
                  {"resignDate": {"$gte": query_end}}],
          "roles": {"$nin": ["organization-manager"]},
          "isDeleted": {"$ne": True}},
         {"organization": org,
          "patient": {"$in": clients_list},
          "createdDate": {"$lt": query_end},
          "$or": [{"finishedDate": None},
                  {"finishedDAte": {"$gte": query_end}}]}],
        [{"sex": 1,
          "birthday": 1,
          "firstName": 1,
          "lastName": 1},
         {"sex": 1,
          "jobTitle": 1,
          "jobType": 1,
          "roles": 1,
          "employType": 1},
         {"patient": 1,
          "type": 1}]
    )

    # Preprocess Section
    if in_file_client.empty:
        raise ReportEmptyError("查詢區間內查無在案之住民。")
    elif staff.empty:
        raise ReportEmptyError("查詢區間內查無在職之工作人員。")

    in_file_client["length"] = count_age(in_file_client["open_at"], end)
    in_file_client["length_group"] = pd.cut(
        in_file_client["length"],
        bins=[-float("inf"), 0.99, 1.99, 2.99, 4.99, 9.99, float("inf")],
        labels=["0~12個月",
                "1年以上未滿2年",
                "2年以上未滿3年",
                "3年以上未滿5年",
                "5年以上未滿10年",
                "10年以上",
                ]
    )

    if any(patient["birthday"].isna()):
        no_bday = patient.loc[patient["birthday"].isna(), "_id"].to_list()
        raise ReportGenerateError(f"無法產製報表，因有住民未填寫生日。{no_bday}")
    patient["birthday"] = trans_timezone(
        patient["birthday"],
        from_utc=8,
        to_utc=0,
    )
    patient["age"] = count_age(patient["birthday"], end)
    patient["age_group"] = pd.cut(
        patient["age"],
        bins=[0, 20, 40, 60, 80, 100, float("inf")],
        labels=["0~20", "21~40", "41~60", "61~80", "81~100", "100~"]
    )
    staff = staff.explode("roles")

    for col in ("patient", "type"):
        if col not in tube.columns:
            tube[col] = None

    # Merging section
    patient = pd.merge(
        patient,
        in_file_client,
        how="left",
        right_on="patient",
        left_on="_id"
    )

    # Computing section
    result = pd.DataFrame(
        [["住民人數",
          "女性",
          len(patient[patient["sex"] == "female"])
          ],
         ["住民人數",
          "男性",
          len(patient[patient["sex"] == "male"])
          ],
         ["住民人數",
          "0~12個月",
          len(patient[patient["length_group"] == "0~12個月"])
          ],
         ["住民人數",
          "1年以上未滿2年",
          len(patient[patient["length_group"] == "1年以上未滿2年"])
          ],
         ["住民人數",
          "2年以上未滿3年",
          len(patient[patient["length_group"] == "2年以上未滿3年"])
          ],
         ["住民人數",
          "3年以上未滿5年",
          len(patient[patient["length_group"] == "3年以上未滿5年"])
          ],
         ["住民人數",
          "5年以上未滿10年",
          len(patient[patient["length_group"] == "5年以上未滿10年"])
          ],
         ["住民人數",
          "10年以上",
          len(patient[patient["length_group"] == "10年以上"])
          ],
         ["住民年齡",
          "女性平均年齡",
          mean(patient[patient["sex"] == "female"]["age"])
          ],
         ["住民年齡",
          "男性平均年齡",
          mean(patient[patient["sex"] == "male"]["age"])
          ],
         ["住民年齡",
          "0~20",
          len(patient[patient["age_group"] == "0~20"])
          ],
         ["住民年齡",
          "21~40",
          len(patient[patient["age_group"] == "21~40"])
          ],
         ["住民年齡",
          "41~60",
          len(patient[patient["age_group"] == "41~60"])
          ],
         ["住民年齡",
          "61~80",
          len(patient[patient["age_group"] == "61~80"])
          ],
         ["住民年齡",
          "81~100",
          len(patient[patient["age_group"] == "81~100"])
          ],
         ["住民年齡",
          "100~",
          len(patient[patient["age_group"] == "100~"])
          ],
         ["住民管路",
          "鼻胃管",
          len(tube[tube["type"] == "NG"])
          ],
         ["住民管路",
          "導尿管",
          len(tube[tube["type"] == "foley"])
          ],
         ["工作人員統計",
          "女性",
          len(staff[staff["sex"] == "female"])
          ],
         ["工作人員統計",
          "男性",
          len(staff[staff["sex"] == "male"])
          ],
         ["工作人員統計",
          "女性護理人員",
          len(staff[(staff["sex"] == "female")
                    & (staff["roles"] == "nurse-practitioner")])
          ],
         ["工作人員統計",
          "男性護理人員",
          len(staff[(staff["sex"] == "male")
                    & (staff["roles"] == "nurse-practitioner")])
          ],
         ["工作人員統計",
          "專任社工師(員)",
          len(staff[(staff["roles"] == "social-worker")
                    & (staff["employType"] == "fullTime")])
          ],
         ["工作人員統計",
          "專任物理治療師",
          len(staff[(staff["jobTitle"] == "物理治療師")
                    & (staff["employType"] == "fullTime")])
          ],
         ["工作人員統計",
          "兼任物理治療師",
          len(staff[(staff["jobTitle"] == "物理治療師")
                    & (staff["employType"] == "partTime")])
          ],
         ["工作人員統計",
          "專任職能治療師",
          len(staff[(staff["jobTitle"] == "職能治療師")
                    & (staff["employType"] == "fullTime")])
          ],
         ["工作人員統計",
          "兼任職能治療師",
          len(staff[(staff["jobTitle"] == "職能治療師")
                    & (staff["employType"] == "partTime")])
          ],
         ["工作人員統計",
          "專任營養師",
          len(staff[(staff["roles"] == "dietitian")
                    & (staff["employType"] == "fullTime")])
          ],
         ["工作人員統計",
          "兼任營養師",
          len(staff[(staff["roles"] == "dietitian")
                    & (staff["employType"] == "partTime")])
          ],
         ["工作人員統計",
          "本籍男性照服員",
          len(staff[(staff["roles"] == "care-giver")
                    & (staff["sex"] == "male")])
          ],
         ["工作人員統計",
          "本籍女性照服員",
          len(staff[(staff["roles"] == "care-giver")
                    & (staff["sex"] == "female")])
          ],
         ["工作人員統計", "外籍男性照服員",
          len(staff[(staff["roles"] == "foreign-nurse-aide")
                    & (staff["sex"] == "male")])],
         ["工作人員統計",
          "外籍女性照服員",
          len(staff[(staff["roles"] == "foreign-nurse-aide")
                    & (staff["sex"] == "female")])
          ]
         ],
        columns=["分類", "項目", "數值"]
    )

    # Filing section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "基本資料"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="基本資料", index=False)
        cell_to_merge = ["A2:A9", "A10:A17", "A18:A19", "A20:A34"]
        for cell in cell_to_merge:
            worksheet.merge_cells(cell)
            worksheet[cell.split(":")[0]].alignment = \
                styles.Alignment(horizontal="center", vertical="center")
        for column in ["A", "B", "C"]:
            worksheet.column_dimensions[column].width = 20

    return report_file
