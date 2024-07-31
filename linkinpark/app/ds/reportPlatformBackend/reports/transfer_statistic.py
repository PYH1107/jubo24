"""
ReportName: 住民統計
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

import pandas as pd
from bson.objectid import ObjectId
from dateutil.relativedelta import relativedelta

from linkinpark.app.ds.reportPlatformBackend.utils import (
    get_nis_data, preprocess_date, trans_timezone)


def transfer_statistic(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    """
    This function will generate a statistic report of each case transfer
    status's person amount.
    :param org: The id of organization for this report.
    :param start: Start date of report period.
    :param end: End date of report period.
    :param suffix: Not in use for this function.
    :return: The report generated.
    """

    # Querying section
    _ = suffix
    start, end, query_start, query_end = preprocess_date([start, end])
    patient_df = get_nis_data(
        "patients",
        {"organization": org, "isDeleted": {"$ne": True}},
        {"sex": 1})
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
                            "absent",
                            ]
                    }
         },
        {"_id": 0, "createdDate": 1, "patient": 1, "status": 1}
    )
    scope = pd.DataFrame(
        pd.Series(
            pd.date_range(start, end - relativedelta(days=1), freq="d")
        ).dt.date).rename(columns={0: "date"})

    # Preprocess section
    if len(transfer_df) == 0:
        result = pd.DataFrame(
            columns=("日期",
                     "在院",
                     "男生",
                     "女生",
                     "住院",
                     "請假",
                     "返回",
                     "新入住",
                     "結案",
                     )
        )
        report_file = BytesIO()
        with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
            result.to_excel(writer, sheet_name="住民統計", index=False)

        return report_file

    transfer_df["createdDate"] = trans_timezone(
        transfer_df.createdDate, from_utc=0, to_utc=8, date_only=True
    )
    transfer_df = transfer_df.sort_values(["patient", "createdDate"])
    transfer_df["start_date"] = transfer_df["createdDate"]
    transfer_df["end_date"] = (
        transfer_df.groupby(["patient"])["createdDate"].shift(-1)
    )
    transfer_df["end_date"] = transfer_df["end_date"].fillna(end)
    transfer_df["key"] = 1
    scope["key"] = 1

    # Merging section
    result = pd.merge(scope, transfer_df, on="key").drop(labels="key", axis=1)
    result = result[(result["start_date"] <= result["date"])
                    & (result["date"] < result["end_date"])]
    result = result[~((result["status"] == "discharge")
                      & (result["start_date"] < result["date"]))]
    result = (
        pd.merge(
            result, patient_df, how="left", left_on="patient", right_on="_id")
        .drop(labels="_id", axis=1)
    )

    # Computing section
    days = list(scope.iloc[:, 0])
    amount, male, female, hosp, absent, back, new, discharge = \
        ([] for _ in range(8))
    for day in days:
        hosted = result[(result["date"] == day)
                        & ~(result["status"] == "discharge")]
        amount.append(len(hosted))
        male.append(len(hosted[hosted["sex"] == "male"]))
        female.append(len(hosted[hosted["sex"] == "female"]))
        hosp.append(len(hosted[hosted["status"].isin(["unplannedHosp",
                                                      "hospTransfer"])]))
        absent.append(len(hosted[hosted["status"] == "absent"]))
        back.append(len(hosted[(hosted["status"] == "return")
                               & (hosted["createdDate"] == day)]))
        new.append(len(hosted[(hosted["status"] == "newcomer")
                              & (hosted["createdDate"] == day)]))
        discharge.append(len(result[(result["status"] == "discharge")
                                    & (result["date"] == day)]))
    result = pd.DataFrame(
        [days, amount, male, female, hosp, absent, back, new, discharge]
    ).T

    # Formatting section
    result = result.rename(columns={0: "日期",
                                    1: "在院",
                                    2: "男生",
                                    3: "女生",
                                    4: "住院",
                                    5: "請假",
                                    6: "返回",
                                    7: "新入住",
                                    8: "結案",
                                    }
                           )
    result["日期"] = pd.to_datetime(result["日期"]).dt.strftime("%m/%d")

    # Filing section
    report_file = BytesIO()
    with pd.ExcelWriter(report_file, engine="openpyxl") as writer:
        result.to_excel(writer, sheet_name="住民統計", index=False)

    return report_file
