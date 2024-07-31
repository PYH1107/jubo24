from pathlib import Path

from linkinpark.app.ds.reportPlatformBackend.utils.appUtils.get_setting \
    import get_report_docstring


def export_markdown_as_text():
    path = Path(__file__).resolve().parent.parent.parent.joinpath("README.md")
    with open(path, "r") as file:
        markdown_text = file.read()
    return markdown_text


def get_available_report():
    reports = get_report_docstring("ReportName")
    text = ""
    for key, value in reports.items():
        text += f"* {value} ({key})\n"
    return text


def generate_description():
    main_body = export_markdown_as_text()
    reports = get_available_report()
    return main_body + "\n## Current available reports\n" + reports
