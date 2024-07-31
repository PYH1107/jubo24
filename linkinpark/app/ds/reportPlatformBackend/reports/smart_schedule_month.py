"""
ReportName: 智慧排程活動月報表
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from dateutil.relativedelta import relativedelta
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportGenerateError, get_nis_data, trans_timezone, preprocess_date)


def smart_schedule_month(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a calendar of activities during the report
    period.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    if (end - start).days < 28:
        raise ReportGenerateError("此為報表月報表，查詢區間不得小於一個月。")

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    activity = get_nis_data(
        "smartschedules",
        {"organization": org,
         "category": "activity",
         "deleted": False,
         "startTime": {"$lt": query_end},
         "endTime": {"$gte": query_start}
         },
        {"_id": 0,
         "task": 1,
         "startTime": 1,
         "endTime": 1}
    )

    # Preprocess Section
    if len(activity) < 1:
        activity = pd.DataFrame(columns=("task", "startTime", "endTime"))
    for column in ["startTime", "endTime"]:
        activity[column] = pd.to_datetime(activity[column])
        activity[column] = trans_timezone(
            activity[column], from_utc=0, to_utc=8)
    try:
        activity["content"] = (activity["startTime"].dt.strftime("%H:%M")
                               + "-"
                               + activity["endTime"].dt.strftime("%H:%M")
                               + "\n"
                               + activity["task"])
    except AttributeError:
        activity["content"] = activity["task"]

    result = pd.DataFrame(
        pd.Series(
            pd.date_range(
                start,
                end - relativedelta(days=1),
                freq="d"))) \
        .rename(columns={0: "content"})
    for col in ["task", "startTime", "endTime"]:
        result[col] = result["content"]
    result["content"] = result["content"].dt.strftime("%m月%d日")
    result = pd.concat([result, activity])
    result = (result.sort_values(["startTime", "endTime"])
              .reset_index(drop=True))
    result["weekday"] = result["startTime"].dt.day_name()
    result["week"] = result["startTime"].dt.isocalendar().week
    min_week = result.at[0, "week"]
    max_week = max(result["week"])
    if min_week == max_week:
        result.loc[result["week"] == min_week, "week"] = 0
    result = result[["week", "weekday", "content"]]
    result["gid"] = (result.groupby(["weekday", "week"])["week"]
                     .rank(method="first"))
    row_skip = list(result.groupby("week")["gid"].max("gid"))
    row_to_bold = []
    for i in range(len(row_skip) + 1):
        row = sum(row_skip[:i])
        row_to_bold.append(row + 2)
    result = result.pivot(
        index=("week", "gid"),
        columns="weekday",
        values="content",
    )

    # Merging section
    # No data need to be merge in this report

    # Computing section
    weekdays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday"
    ]
    for weekday in weekdays:
        if weekday not in result.columns:
            result[weekday] = None
    result = result[weekdays]
    result = result.rename(
        columns={"Monday": "星期一",
                 "Tuesday": "星期二",
                 "Wednesday": "星期三",
                 "Thursday": "星期四",
                 "Friday": "星期五",
                 "Saturday": "星期六",
                 "Sunday": "星期日"}
    )

    # Filing section
    file = BytesIO()
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "智慧排程活動月報表"
    with pd.ExcelWriter(file, engine="openpyxl") as writer:
        writer.book = workbook
        writer.sheets = \
            dict((ws.title, ws) for ws in workbook.worksheets)
        result.to_excel(writer, sheet_name="智慧排程活動月報表", index=False)
        columns = ["A", "B", "C", "D", "E", "F", "G"]
        for column in columns:
            worksheet.column_dimensions[column].width = 40

        for row in worksheet[2:worksheet.max_row]:
            for col in range(7):
                cell = row[col]
                cell.alignment = Alignment(wrap_text=True)

        for row_num in row_to_bold:
            row = worksheet[int(row_num)]
            for col in range(7):
                cell = row[col]
                cell.font = Font(bold=True)

    return file
