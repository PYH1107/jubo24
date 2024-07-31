"""
ReportName: 錯誤測試用報告
POC: Shen Chiang
"""

from datetime import datetime
from io import BytesIO

from bson.objectid import ObjectId
from linkinpark.app.ds.reportPlatformBackend.utils import ReportGenerateError


def error_report(
        org: ObjectId, start: datetime, end: datetime, suffix: str = None
) -> BytesIO:
    _ = (org, start, end, suffix)
    error = True
    if error:
        raise ReportGenerateError("這是一份用來示範錯誤訊息的報表。")
    report_file = BytesIO()
    return report_file
