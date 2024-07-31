"""
ReportName: 匯出檢驗報告
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from linkinpark.app.ds.reportPlatformBackend.utils import (
    ReportEmptyError, get_nis_data, preprocess_date)


def export_exam_report(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will export all exam report record for the assigned
    organization for a specific period defined by the start and end variables.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """
    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    settings, patient_df, exam_df = get_nis_data(
        ["settings", "patients", "examreport2"],
        [{"organization": org, "moduleName": "examReport2"},
         {"organization": org},
         {"organization": org,
          "createdDate": {"$gte": query_start,
                          "$lt": query_end}}],
        [{"_id": 0, "value": 1},
         {"branch": 1, "room": 1, "bed": 1, "lastName": 1, "firstName": 1},
         {"createdDate": 1, "examData": 1, "patient": 1}],
    )

    # Preprocess Section
    if exam_df.empty:
        raise ReportEmptyError("查詢區間內查無相關檢驗報告。")
    settings = settings["value"][0]
    item_labels = {}
    for item_dict in settings:
        grand_label = item_dict["label"][0:2]
        if "items" not in item_dict:
            continue
        for item in item_dict["items"]:
            if "fieldName" in item:
                item_labels.update(
                    {item["fieldName"]: grand_label + "-" + item["label"]}
                )
    patient_df.rename(columns={"_id": "patient"}, inplace=True)
    patient_df["fullname"] = patient_df["lastName"] + patient_df["firstName"]
    exam_df["createdDate"] = exam_df["createdDate"] + pd.Timedelta(hours=8)
    exam_df = pd.merge(
        exam_df, exam_df["examData"].apply(pd.Series), left_index=True,
        right_index=True,
    )

    # Merging section
    result = pd.merge(patient_df, exam_df, "right", on="patient")

    # Computing section
    keep_col = ["branch", "room", "bed", "fullname", "createdDate"]
    exam_col = [item for item in item_labels.keys() if item in result.columns]
    for item in exam_col:
        unit = item + "Unit"
        if unit in result.columns:
            unit_value = result.loc[~result[unit].isna(), unit].unique()
            if unit_value.__len__() == 1 and unit_value:
                keep_col.append(item)
                item_labels[item] = item_labels[item] + "\n(" + unit_value[0] + ")"
            elif unit_value.__len__() > 1:
                keep_col.extend([item, unit])
                item_labels[unit] = item_labels[item] + "\n(單位)"
            else:
                keep_col.append(item)
        else:
            keep_col.append(item)
    item_labels.update({
        "branch": "院區",
        "room": "房號",
        "bed": "床號",
        "fullname": "姓名",
        "createdDate": "檢驗日期"
    })
    result = result[keep_col].rename(columns=item_labels)

    # Filing section
    report_file = BytesIO()
    result.to_excel(report_file, index=False)

    return report_file
