from datetime import datetime, timedelta


def get_iso_day_string(backtrack_day=0):
    '''
    A function converting current time to iso day string.
    It also supports day backtrack usage that ensuring your date-related pipeline worked.

    For example, '2021-09-16' is an iso day string.
    Backtracking the time with 3 day will be '2021-09-13'.


    Parameter:
    ----------
    backtrack_day : int, default=0
        The number of day for backtracking.

    Returns:
    --------
    iso_day_string : str
        The string of the iso day with backtracking.
    '''
    the_time = datetime.now()
    the_time = the_time - timedelta(days=backtrack_day)
    iso_day_string = datetime.strftime(the_time, '%Y-%m-%d')
    return iso_day_string
