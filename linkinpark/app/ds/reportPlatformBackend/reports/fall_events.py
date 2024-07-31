"""
ReportName: 跌倒紀錄月報
POC: Lucy Chou
"""

import datetime
from io import BytesIO

import pandas as pd
from bson import ObjectId
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.worksheet.page import PageMargins

from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportGenerateError, get_nis_data, preprocess_date, trans_timezone)


def fall_events(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a report of fall events for 彰化養護.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Regulation of this report
    if org != ObjectId("60ebc7a55738dd0028cd6bf9"):
        raise ReportGenerateError(
            "此為彰化養護專屬報表，因為院區設定問題、無法應用於其他機構。")

    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])

    # Querying section
    patients = get_nis_data(
        "patients",
        {"organization": org, "isDeleted": {"$ne": True}},
        {"lastName": 1, "firstName": 1, "branch": 1, "bed": 1, "room": 1})
    patients_list = patients["_id"].to_list()
    fall, transfer_manages, organization = get_nis_data(
        ("fallevents", "transfermanages", "organizations"),
        (
            {
                "organization": org,
                "patient": {"$in": patients_list},
                "createdDate": {
                    "$gte": query_start,
                    "$lt": query_end
                },
            },
            {
                "organization": org,
                "patient": {"$in": patients_list},
                "createdDate": {
                    "$lt": query_end
                },
                "status": {
                    "$in": [
                        "newcomer",
                        "hospTransfer",
                        "unplannedHosp",
                        "return",
                        "discharge",
                        "absent",
                    ]
                },
            },
            {
                "_id": org
            },
        ),
        (
            {
                "_id": 0,
                "createdDate": 1,
                "patient": 1,
                "location": 1,
                "causes": 1,
                "injury": 1,
                "repeatedness": 1,
            },
            {
                "patient": 1,
                "status": 1,
                "bedHold": 1,
                "createdDate": 1
            },
            {
                "nickName": 1,
                "preference": 1
            },
        ),
    )

    # Preprocess Section
    org_df = organization
    organization = organization["preference"].apply(pd.Series)
    shift = pd.DataFrame(organization["shifts"].values.tolist()).T
    shift = shift[0].apply(pd.Series)[["name", "timeStart", "timeEnd"]]
    time_col = ["timeStart", "timeEnd"]
    for item in time_col:
        shift[item] = pd.to_datetime(shift[item]).dt.time

    fall["createdDate"] = trans_timezone(
        fall["createdDate"], from_utc=0, to_utc=8)
    fall["time"] = fall["createdDate"].dt.time

    shift_dict = {"白班": 0, "小夜": 1, "大夜": 2}
    for row in range(fall.shape[0]):
        for key, value in shift_dict.items():
            shift_start, shift_end = shift.loc[value, "timeStart"], shift.loc[
                value, "timeEnd"]
            if (shift_start <= fall.loc[row, "time"] <= shift_end):
                fall.loc[row, "shift"] = key

    patients["name"] = patients["lastName"] + patients["firstName"]

    fall = pd.merge(fall, patients, left_on="patient",
                    right_on="_id").drop(columns=["lastName", "firstName"])

    fall["causes"] = fall["causes"].str.replace(
        ".*[^causeHealth|^causeEnvironment|^causeTreatment].*", "other")

    # Computing Section
    empty_branch = pd.DataFrame(
        columns=["1C", "1D", "1E", "2C", "2D", "2E", "3C", "3D", "3E"])

    count_status = ["newcomer", "return"]
    other_status = ["hospTransfer", "unplannedHosp", "discharge", "absent"]
    transfer_manages['createdDate'] = trans_timezone(
        transfer_manages["createdDate"], from_utc=0, to_utc=8)
    transfer_manages.loc[
        transfer_manages.status.isin(count_status), "count"] = "yes"
    transfer_manages.loc[
        transfer_manages.status.isin(other_status), "count"] = "no"
    transfer_manages = transfer_manages.sort_values(["patient", "createdDate"])
    transfer_manages["endDate"] = transfer_manages.groupby(
        ["patient"])["createdDate"].shift(-1)
    transfer_manages["key"] = 1

    date_df = pd.DataFrame(
        pd.Series(
            pd.date_range(query_start,
                          query_end - relativedelta(days=1),
                          freq="D")).dt.date,
        columns=["date"],
    )
    date_df["key"] = 1

    result_sheet = pd.merge(date_df, transfer_manages,
                            on="key").drop(labels="key", axis=1)
    result_sheet = pd.merge(result_sheet,
                            patients,
                            left_on="patient",
                            right_on="_id")
    result_sheet = result_sheet[
        (result_sheet["createdDate"].dt.date <= result_sheet["date"])
        & (result_sheet["count"] == "yes")
        & ((result_sheet["date"] < result_sheet["endDate"].dt.date)
           | (pd.isna(result_sheet["endDate"])))]

    result_sheet = result_sheet.groupby(["branch"
                                         ]).agg({"name": ["count", "nunique"]})
    """
    result_sheet will return a dataframe with a hierarchical index.
    Rename the index by the following code.
    """
    result_sheet.columns = [
        "_".join(str(s).strip() for s in col if s)
        for col in result_sheet.columns
    ]

    fall_count = (fall.groupby(["branch"]).agg({
        "_id": "count",
        "patient": "nunique"
    }).rename(columns={
        "_id": "fall_count",
        "patient": "patient_count"
    }))
    repeat_count = (
        fall[fall["repeatedness"] == "repeatYes"].groupby(["branch"]).agg(
            {"_id": "nunique"}).rename(columns={"_id": "repeat_count"}))

    # fall events
    df_list = {"causes", "injury", "shift", "location"}
    df = pd.DataFrame()
    for item in df_list:
        temp = pd.pivot_table(fall,
                              values="patient",
                              index=item,
                              columns="branch",
                              aggfunc="count")
        df = pd.concat([df, temp])

    df = pd.concat(
        [empty_branch, result_sheet.T, fall_count.T, repeat_count.T,
         df]).fillna(0)

    row = [
        "causeHealth",
        "causeTreatment",
        "causeEnvironment",
        "other",
        "injuryLevel1",
        "injuryLevel2",
        "injuryLevel3",
        "白班",
        "小夜",
        "大夜",
        "bedside",
        "bathroom",
        "hallway",
        "saloon",
    ]
    for item in row:
        if item not in df.index:
            df.loc[item] = 0

    df = df.fillna(0)
    df["全院區"] = df.apply(lambda x: x.sum(), axis=1)

    for row in df.index:
        df.loc[row, "康樂家園"] = (
            df.loc[row, "1C"] + df.loc[row, "1D"] + df.loc[row, "1E"])
        df.loc[row, "養護區"] = (
            df.loc[row, "2C"] + df.loc[row, "2D"] + df.loc[row, "2E"])
        df.loc[row, "長照區"] = (
            df.loc[row, "3C"] + df.loc[row, "3D"] + df.loc[row, "3E"])

    cause_dict = {
        "health_rate": "causeHealth",
        "treatment_rate": "causeTreatment",
        "enviro_rate": "causeEnvironment",
        "other_rate": "other",
    }
    injury_dict = {
        "injury1_rate": "injuryLevel1",
        "injury2_rate": "injuryLevel2",
        "injury3_rate": "injuryLevel3",
    }
    for col in range(df.shape[1]):
        df.loc["fall_rate", df.columns[col]] = (
            df.loc["fall_count", df.columns[col]] /
            df.loc["name_count", df.columns[col]]) * 100
        df.loc["repeat_rate", df.columns[col]] = (
            df.loc["repeat_count", df.columns[col]] /
            df.loc["patient_count", df.columns[col]]) * 100
        df.loc["cause_sum", df.columns[col]] = (
            df.loc["causeHealth", df.columns[col]] +
            df.loc["causeTreatment", df.columns[col]] +
            df.loc["causeEnvironment", df.columns[col]] +
            df.loc["other", df.columns[col]])
        df.loc["injury_sum", df.columns[col]] = (
            df.loc["injuryLevel1", df.columns[col]] +
            df.loc["injuryLevel2", df.columns[col]] +
            df.loc["injuryLevel3", df.columns[col]])

        df.loc["injury_rate", df.columns[col]] = (
            df.loc["injury_sum", df.columns[col]] /
            df.loc["cause_sum", df.columns[col]]) * 100
        for key, value in cause_dict.items():
            df.loc[key, df.columns[col]] = (
                df.loc[value, df.columns[col]] /
                df.loc["cause_sum", df.columns[col]]) * 100
        for key, value in injury_dict.items():
            df.loc[key, df.columns[col]] = (
                df.loc[value, df.columns[col]]
                / df.loc["injury_sum", df.columns[col]]) * 100

    df_mapping = (pd.DataFrame({
        "index_name": [
            "name_count",
            "name_nunique",
            "fall_count",
            "fall_rate",
            "repeat_count",
            "patient_count",
            "repeat_rate",
            "causeHealth",
            "health_rate",
            "causeTreatment",
            "treatment_rate",
            "causeEnvironment",
            "enviro_rate",
            "other",
            "other_rate",
            "injury_sum",
            "injury_rate",
            "injuryLevel1",
            "injury1_rate",
            "injuryLevel2",
            "injury2_rate",
            "injuryLevel3",
            "injury3_rate",
            "白班",
            "小夜",
            "大夜",
            "bedside",
            "bathroom",
            "hallway",
            "saloon",
        ]
    }).reset_index().set_index("index_name"))
    df = df.fillna(0).reset_index()
    df["index_num"] = df["index"].map(df_mapping["index"])
    df = (df.sort_values("index_num").fillna(0).drop(
        df[df["index"].isin(["injuryNo", "cause_sum"])].index, axis=0))
    df = df[[
        "index",
        "康樂家園",
        "2C",
        "2D",
        "2E",
        "3C",
        "3D",
        "3E",
        "全院區",
        "養護區",
        "長照區"
    ]]

    location = {
        "bedside": "床邊",
        "bathroom": "浴廁",
        "hallway": "走廊",
        "saloon": "交誼廳"
    }
    df = df.replace({"index": location}, regex=True).round(2)

    # Filling Section
    report_file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = f"{start.year}-{start.month}-月報表"

    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = dict((ws.title, ws) for ws in workbook.worksheets)
        df.iloc[:26].to_excel(
            writer,
            sheet_name=f"{start.year}-{start.month}-月報表",
            index=False,
            header=True,
            startrow=1,
            startcol=2,
        )
        df.iloc[26:].to_excel(
            writer,
            sheet_name=f"{start.year}-{start.month}-月報表",
            index=False,
            header=False,
            startrow=28,
            startcol=2,
        )

    worksheet.page_margins = PageMargins(
        left=0.25,
        right=0.25,
        top=0.5,
        bottom=0.5,
        header=0.25,
        footer=0.25,
    )

    worksheet[
        "A1"] = f"{org_df.loc[0, 'nickName']}跌倒統計-{start.year}年{start.month}月報表"
    worksheet["A3"] = "當月住民總人日數"
    worksheet["A4"] = "收容人次"
    worksheet["A5"] = "發生件數"
    worksheet["A6"] = "跌倒發生率(%)"
    worksheet["A7"] = "當月重複跌倒"
    worksheet["A8"] = "有紀錄跌倒"
    worksheet["A9"] = "當月重複跌倒"
    worksheet["A10"] = "跌倒原因"
    worksheet["A18"] = "跌倒造成傷害"
    worksheet["A20"] = "嚴重度"
    worksheet["A26"] = "跌倒班別"
    worksheet["A29"] = "發生地點"
    worksheet["B10"] = "健康因素"
    worksheet["B12"] = "治療或藥物"
    worksheet["B14"] = "環境"
    worksheet["B16"] = "其他"
    worksheet["B20"] = "1級"
    worksheet["B22"] = "2級"
    worksheet["B24"] = "3級"
    worksheet["B26"] = "白班"
    worksheet["B27"] = "小夜"
    worksheet["B28"] = "大夜"

    col_name = {
        "人數": ["C7", "C8"],
        "比率": ["C9", "C11", "C13", "C15", "C17", "C19", "C21", "C23", "C25"],
        "件數": ["C10", "C12", "C14", "C16", "C18", "C20", "C22", "C24"],
    }
    for key, value in col_name.items():
        for i in value:
            worksheet[i] = key

    for cell in (
        "A1:M1",
        "A2:C2",
        "A3:C3",
        "A4:C4",
        "A5:C5",
        "A6:C6",
        "A7:B7",
        "A8:B8",
        "A9:B9",
        "A10:A17",
        "B10:B11",
        "B12:B13",
        "B14:B15",
        "B16:B17",
        "A18:B19",
        "A20:A25",
        "B20:B21",
        "B22:B23",
        "B24:B25",
        "A26:A28",
        "B26:C26",
        "B27:C27",
        "B28:C28",
        "A29:B33",
    ):
        worksheet.merge_cells(cell)

    worksheet["A1"].font = Font(size=14, bold=True)
    worksheet["A1"].alignment = Alignment(horizontal="center",
                                          vertical="center")

    report_file = BytesIO()
    workbook.save(report_file)
    workbook.close()

    return report_file
