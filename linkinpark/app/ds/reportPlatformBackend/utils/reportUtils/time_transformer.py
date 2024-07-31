from datetime import date, datetime

import pandas as pd
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta


def trans_to_date(target: (str, tuple, list, date)) -> (list, date):
    """
    This function will transfer the target variable to date object or a list
    of date object.
    :param target: The variable to transfer.
    :return: Variable transferred to date object.
    """

    if isinstance(target, date):
        result = parse(str(target)).date()
    elif isinstance(target, str):
        result = parse(target).date()
    elif hasattr(target, "__iter__"):
        result = []
        for item in target:
            if isinstance(item, date):
                result.append(item)
            else:
                result.append(parse(item).date())
    else:
        raise AttributeError(f"target's attribute must not be {type(target)}.")
    return result


def trans_timezone(times: (datetime, list),
                   from_utc: int,
                   to_utc: int,
                   date_only: bool = False,
                   ignore_nan: bool = False
                   ) -> (date, datetime, list):
    """
    This function serves as a time zone converter, which helps you to get the
    relative time in different time zones.
    :param times: The time to be convert.
    :param from_utc: The original time zone in UTC.
    :param to_utc: The target time zone to transfer in UTC.
    :param date_only: True to return date, False to return datetime.
    :param ignore_nan: If True, the function will ignore nan values.
    :return: The transferred time.
    """

    distance = from_utc - to_utc
    if isinstance(times, datetime):
        if not pd.isna(times):
            result = times - relativedelta(hours=distance)
        elif ignore_nan:
            result = times
        else:
            raise ValueError("input times with value NaT.")

        if date_only:
            result = result.date()
    else:
        result = []
        for tm in times:
            if not pd.isna(tm):
                new_time = tm - relativedelta(hours=distance)
            elif ignore_nan:
                new_time = tm
            else:
                raise ValueError("input times with value NaT.")
            if date_only:
                new_time = new_time.date()
            result.append(new_time)

    return result


def preprocess_date(date_list):
    date_list = trans_to_date(date_list)
    trans_date_list = trans_timezone(date_list, from_utc=8, to_utc=0)
    result = date_list + trans_date_list
    return result
