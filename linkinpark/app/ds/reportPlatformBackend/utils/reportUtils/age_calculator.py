from datetime import date, datetime

from linkinpark.app.ds.reportPlatformBackend.utils.reportUtils.time_transformer \
    import trans_to_date


def after_birthday(birthday: (datetime, date),
                   standard_date: (datetime, date)
                   ) -> bool:
    """
    This function will check if the standard date is after that year's
    birthday or not and return a boolean value representing it.
    :param birthday: The birthday to check.
    :param standard_date: The standard date to be check.
    :return: True if standard date is after that year's birthday.
    """

    birthday = birthday.strftime("%m%d")
    standard_date = standard_date.strftime("%m%d")
    if standard_date >= birthday:
        return True
    else:
        return False


def single_age_count(birthday: (datetime, date),
                     standard_date: (datetime, date)
                     ) -> int:
    """
    This function will count the age of a person between two dates.
    :param birthday: The person's birthday.
    :param standard_date: The date to count the person's age.
    :return: The person's age.
    """
    if after_birthday(birthday, standard_date):
        result = standard_date.year - birthday.year
    else:
        result = standard_date.year - birthday.year - 1

    return result


def count_age(birthday: (datetime, date, tuple, list),
              standard_date: (datetime, date, tuple, list)
              ) -> (int, list):
    """
    This function will count the age by subtracting the standard date and
    birthday.
    :param birthday: The date to be subtracted.
    :param standard_date: The standard date for calculating the age.
    :return: Age at the standard date.
    """

    birthday = trans_to_date(birthday)
    standard_date = trans_to_date(standard_date)

    # both birthday and standard date are single value.
    if not (hasattr(birthday, "__iter__")
            or hasattr(standard_date, "__iter__")):
        result = single_age_count(birthday, standard_date)

    # multi birthday and one standard date.
    elif (hasattr(birthday, "__iter__")
          and not hasattr(standard_date, "__iter__")):
        result = []
        for day in birthday:
            result.append(single_age_count(day, standard_date))

    # single birthday with multi standard date.
    elif (not hasattr(birthday, "__iter__")
          and hasattr(standard_date, "__iter__")):
        result = []
        for day in standard_date:
            result.append(single_age_count(standard_date, day))

    # both birthday and standard date has multiple value.
    elif len(birthday) == len(standard_date):
        result = []
        for num in range(len(birthday)):
            result.append(single_age_count(standard_date[num], birthday[num]))

    else:
        raise ValueError(
            f"The length of input variable standard_date must "
            f"equals to 1 or same as the input variable birthday."
        )

    return result
