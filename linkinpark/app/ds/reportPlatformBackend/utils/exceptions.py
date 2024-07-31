class ReportGenerateError(Exception):
    """Raised when the report is unable to be generated."""
    pass


class ReportFrequencyError(Exception):
    """Raised when the report frequency is unknown."""
    pass


class ReportMailingError(Exception):
    """Raise when the report cannot be send by e-mail"""
    pass


class ReportEmptyError(Exception):
    """Raise when the report has no valid data"""
    pass


class ReportNameError(BaseException):
    """Raise when the report name was not stated in the report file's
    docstring"""
    pass
