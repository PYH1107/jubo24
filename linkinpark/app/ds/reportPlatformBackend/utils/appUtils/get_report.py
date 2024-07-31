import importlib

from dateutil.relativedelta import relativedelta


def get_function(report_name):
    function = getattr(
        importlib.import_module(
            f"linkinpark.app.ds.reportPlatformBackend.reports.{report_name}"),
        report_name
    )
    return function


def get_report(report_name, params):
    report_function = get_function(report_name)
    file = report_function(
        params["org"],
        params["start"],
        params["end"] + relativedelta(days=1),
        params["suffix"],
    ).getvalue()
    return file
