from datetime import date, datetime
from enum import Enum
from os import listdir
from pathlib import Path
from typing import Union

from fastapi import Query
from pydantic import BaseModel
from linkinpark.app.ds.reportPlatformBackend.utils.exceptions \
    import ReportNameError

FOLDER = Path(__file__).resolve().parent.parent.parent.joinpath("reports")


def report_files():
    files = []
    for file_name in listdir(FOLDER):
        if file_name.startswith("__"):
            continue
        files.append(file_name)
    return files


def get_report_options():
    options = {}
    for file in report_files():
        option = file.split(".")[0]
        options[option] = option
    return options


def get_report_docstring(keyword):
    exceptions = []
    names = {}
    for file in report_files():
        with open(FOLDER.joinpath(file), "r") as file_content:
            line_num, name = 0, None
            while line_num < 10:
                line = file_content.readline()
                if keyword in line:
                    name = (line.split(":")[1]).strip()
                    break
                else:
                    line_num += 1
        names[file.split(".")[0]] = name
        if name is None:
            exceptions.append(file)
    if exceptions:
        error_message = f"No {keyword} found in file {', '.join(exceptions)}"
        raise ReportNameError(error_message)
    return names


class Queries(BaseModel):
    request_id: str = Query(
        title="Request ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24)
    user_id: str = Query(
        title="User ID",
        description="The log in user's ID.")
    request_at: datetime = Query(
        title="Request sent time",
        description="The request time record by front end service.")
    org: str = Query(
        title="Organization ID",
        description="It should be a 24 character composed ObjectID.",
        min_length=24,
        max_length=24)
    start: date = Query(
        title="Start date",
        description="The start date of the querying period in ISO 8601 format.")
    end: date = Query(
        title="End date",
        description="The end date of the querying period in ISO 8601 format.")
    suffix: Union[str, None] = Query(
        title="Suffix",
        description="Additional parameter pass to the report generate "
                    "function. The format should be a JSON like string, "
                    "but key value depends on each report.")

    class Config:
        schema_extra = {
            "example": {
                'request_id': 'idStringSendFromFrontend',
                'user_id': 'userIdProvidedByFrontend',
                'request_at': '2023-02-01T08:00:00.000Z',
                'org': '5c10bdf47b43650f407de7d6',
                'start': '2023-01-01',
                'end': '2023-01-31',
                'suffix': '{"key": "value"}',
            }
        }


class NickName(BaseModel):
    nickname: str = Query(
        title="Organization's nickname",
        description="The nickname of the organization in string format",
        min_length=2)


class FullName(BaseModel):
    name: str = Query(
        title="Organization's name",
        description="The official name of the organization in string format",
        min_length=4
    )


ReportOptions = Enum("ReportOptions", get_report_options())
