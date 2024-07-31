from __future__ import annotations

import collections
import io
import logging
import os
import sys
import traceback
from collections.abc import Mapping
from functools import wraps
from time import perf_counter
from typing import Optional

import psutil
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.logging_v2.client import _GAE_RESOURCE_TYPE, Client
from google.cloud.logging_v2.handlers import (CloudLoggingHandler,
                                              StructuredLogHandler)
from google.cloud.logging_v2.handlers._monitored_resources import \
    detect_resource
from google.cloud.logging_v2.handlers.handlers import (
    _CLEAR_HANDLER_RESOURCE_TYPES, EXCLUDED_LOGGER_DEFAULTS)

# include only essential components when import *
__all__ = (
    'log', 'debug', 'info', 'warning', 'error', 'critical', 'timer', 'monitor',
    'basicConfig', 'setupCloudLogging', 'getDefaultLoggerName', 'CloudLogger', 'getLogger'
)


# TODO: log-based metrics builder
# TODO: replace formatter of logging.root to log labels of logRecord
# TODO: handling stack for log path in logging format
# TODO: review environment variables across infra

DEFAULT_LABELS = {
    "env": "test"
}

DEFAULT_GLOBAL_LOGGER_NAME = 'ai_global_logger'

ENV_NAME__APP_NAME = 'APP_NAME'
ENV_NAME__APP_ENV = 'APP_ENV'
ENV_NAME__OFFLINE_LOGGING = 'OFFLINE_LOGGING'

_CLIENT = None
_RESOURCE = None
cloudLoggerRoot = None


def isEnvironmentValid() -> bool:
    return not (_CLIENT is None or _RESOURCE is None)


def isSetup() -> bool:
    return cloudLoggerRoot is not None


def isUsingAPI() -> bool:
    if isSetup():
        if next((x for x in cloudLoggerRoot.handlers if isinstance(x, CloudLoggingHandler)), None):
            return True
    return False


def setupCloudLogging() -> None:
    """
    Initialize client of google cloud api, retrieve resources, and setup default logger class.
    This will be called automatically when using this module.
    """

    # https://github.com/googleapis/python-logging/blob/main/google/cloud/logging_v2/handlers/_monitored_resources.py
    global _CLIENT
    global _RESOURCE
    global cloudLoggerRoot

    # https://github.com/googleapis/python-cloud-core/blob/main/google/cloud/client/__init__.py
    if not isEnvironmentValid():    
        _CLIENT = Client()
        _RESOURCE = detect_resource(_CLIENT.project)

    if not isSetup():
        cloudLoggerRoot = getLogger()


def loggingBasicConfig():

    logging.root.handlers.clear()
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s (%(filename)s:%(lineno)d) [%(levelname)s] - %(message)s",
        datefmt='%Y-%m-%dT%H:%M:%S%z'
    )


def basicConfig(offline=False) -> None:
    """
    Provide a basic logging setting to regular users,
    will set the logging.root and cloudLogger.root and propagation if needed.

    This function should be call at the beginning of a application.
    """

    global cloudLoggerRoot

    if offline or (ENV_NAME__OFFLINE_LOGGING in os.environ and os.environ[ENV_NAME__OFFLINE_LOGGING].lower() in ('true', '1')):
        cloudLoggerRoot = CloudLogger.manager.getOfflineLogger()
        return

    try:
        setupCloudLogging()

        if isUsingAPI():
            loggingBasicConfig()

            # propagate logs to native root logger and show the logs on console to help debugging 
            cloudLoggerRoot.parent = logging.root
            cloudLoggerRoot.propagate = True

    except DefaultCredentialsError:
        loggingBasicConfig()

        logging.warning(
            "need to setting right GOOGLE_APPLICATION_CREDENTIALS env variable to auth", exc_info=True)

        cloudLoggerRoot = CloudLogger.manager.getPlaceHolder()


def getDefaultLoggerName():

    # TODO: specific environment variables, which are not defined yet
    if ENV_NAME__APP_NAME in os.environ:
        return os.environ[ENV_NAME__APP_NAME]

    # TODO: should specify a common project structure
    # https://stackoverflow.com/a/8663885
    from inspect import stack

    # get the first stack that is not from this file
    caller_path = next(x.filename for x in stack()
                       if x.filename != __file__)

    dirname = os.path.split(os.path.dirname(caller_path))[-1]

    # the cases the running code is not in a project folder
    if dirname not in ('source', 'src', ''):
        return dirname

    # the default hostname of a k8s pod is metadata.name, which is defined in manifest
    if _RESOURCE and _RESOURCE.type == 'k8s_container' and 'hostname' in os.environ:
        return os.environ['hostname']

    # special case, to ignore the file structure as the folder name is not consistent
    if 'linkinpark' in caller_path:
        return 'linkinpark'

    return DEFAULT_GLOBAL_LOGGER_NAME


def getLogger(
        name: Optional[str] = None,
        labels: Optional[Mapping] = None,
        log_level: Optional[int] = logging.INFO) -> CloudLogger:
    return CloudLogger.getLogger(name, labels, log_level)


def _checkAllString(labels):
    if isinstance(labels, collections.abc.Mapping):
        for k, v in labels.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise TypeError("Key value in labels need to be str")
    else:
        raise TypeError("Labels need to be dict type")


def log(level, msg, *args, **kwargs):
    """
    Send log record to cloud logging if valid,
    otherwise, propagate to root logger.
    """

    if not isSetup():
        basicConfig()

    cloudLoggerRoot.log(level, msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
    """
    Write log with level DEBUG.
    """
    log(logging.DEBUG, msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """
    Write log with level INFO.
    """
    log(logging.INFO, msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """
    Write log with level WARNING.
    """
    log(logging.WARNING, msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """
    Write log with level ERROR.
    """
    log(logging.ERROR, msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
    """
    Write log with level CRITICAL.
    """
    log(logging.CRITICAL, msg, *args, **kwargs)


def timer(logger_name=None, labels=None):
    """
    A decorator to time a function.

    args:
        logger_name (str): The name of logger, will be determined by the environment if left None.
        labels (dict): Custom labels , will be mixed with logger labels.

    usage:
        ```python
        @timer(labels={'type': 'timer', 'another_label': 'anything'})
        def some_function()
            some_task(...)

        @timer()
        def some_function_without_args()
            some_task(...)

        # run function directly
        some_function()
        ```

    """
    def _timer(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ts_bf = perf_counter()
            result = func(*args, **kwargs)
            time_used = perf_counter() - ts_bf

            getLogger(logger_name).info({
                'message': f'timed function - {func.__qualname__}: {time_used}',
                'category': 'timer',
                'metrics': {
                    'function': func.__qualname__,
                    'time_used': time_used
                }
            }, labels=labels)
            return result
        return wrapper
    return _timer


def monitor(logger_name=None, labels=None):
    """
    A decorator to monitor a function.
    ref: https://psutil.readthedocs.io/en/latest/#

    args:
        logger_name (str): The name of logger, will be determined by the environment if left None.
        labels (dict): Custom labels , will be mixed with logger labels.

    usage:
        ``` python
        @monitor(labels={'type': 'timer', 'another_label': 'anything'})
        def some_function()
            some_task(...)

        # run function directly
        some_function()
        ```

    """
    def _monitor(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            p = psutil.Process()  # get the operating process
            logger = getLogger(logger_name)

            profile_before = {}
            with p.oneshot():
                profile_before['function'] = func.__qualname__
                profile_before['ts'] = perf_counter()
                profile_before['cpu_times'] = p.cpu_times()
                profile_before['cpu_percent'] = p.cpu_percent()
                profile_before['memory_full_info'] = p.memory_full_info()
                profile_before['memory_percent'] = p.memory_percent()
                profile_before['threads'] = p.threads()
                profile_before['connections'] = p.connections('all')
                logger.debug({
                    'message': f'profile before function - {p.name}({p.pid}):{func.__qualname__}',
                    'category': 'profile',
                    'profile': profile_before
                }, labels=labels)

            try:
                result = func(*args, **kwargs)
            except Exception as _:
                logger.error(f"Error when Executing {func.__name__}.", exc_info=True)
                return

            profile_after = {}
            with p.oneshot():
                profile_after['function'] = func.__qualname__
                profile_after['ts'] = perf_counter()
                profile_after['cpu_times'] = p.cpu_times()
                profile_after['cpu_percent'] = p.cpu_percent()
                profile_after['memory_full_info'] = p.memory_full_info()
                profile_after['memory_percent'] = p.memory_percent()
                profile_after['threads'] = p.threads()
                profile_after['connections'] = p.connections('all')
                logger.debug({
                    'message': f'profile after function - {p.name}({p.pid}):{func.__qualname__}',
                    'category': 'profile',
                    'profile': profile_after
                }, labels=labels)

            logger.info({
                'message': f'monitored function - {func.__qualname__}',
                'category': 'monitor',
                'metrics': {
                    'function': func.__qualname__,
                    'time_usage': profile_after['ts'] - profile_before['ts'],
                    'cpu_usage': profile_after['cpu_percent'] / psutil.cpu_count(),
                    'memory_usage': profile_after['memory_percent'],
                    'connections': len(profile_after['connections']),
                }
            }, labels=labels)

            return result
        return wrapper
    return _monitor


class CloudLogger(logging.getLoggerClass()):
    """
    Logger class implements a logging object based on Google Cloud Logging,
    which facilitates sending log messages to Google Cloud Logging and using
    custom labels to filter log messages.
    """
    @classmethod
    def getLogger(
            cls,
            name: Optional[str] = None,
            labels: Optional[Mapping] = None,
            log_level: Optional[int] = logging.INFO) -> CloudLogger:
        return cls.manager.getLogger(name, labels, log_level)

    # from source code of python/logging/__init__.py
    # is override to attach additional labels after makeRecord and before handling the log
    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, **kwargs):
        """
        Low-level logging routine which creates a LogRecord and then calls
        all the handlers of this logger to handle the record.

        the custom parts are to integrate metrics and labels from either argument or dictionary into log record
        """

        # custom part start
        if 'metrics' in kwargs:
            if isinstance(msg, str):
                msg = {'message': msg, 'metrics': kwargs['metrics']}

            if isinstance(msg, Mapping):
                if 'metrics' in msg:
                    msg['metrics'] = {**msg['metrics'], **kwargs['metrics']}
                else:
                    msg['metrics'] = kwargs['metrics']
        # custom part end

        sinfo = None
        if logging._srcfile:
            # IronPython doesn't track Python frames, so findCaller raises an
            # exception on some versions of IronPython. We trap it here so that
            # IronPython can use logging.
            try:
                fn, lno, func, sinfo = self.findCaller(stack_info)
            except ValueError:  # pragma: no cover
                fn, lno, func = "(unknown file)", 0, "(unknown function)"
        else:  # pragma: no cover
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
        if exc_info:
            if isinstance(exc_info, BaseException):
                exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
            elif not isinstance(exc_info, tuple):
                exc_info = sys.exc_info()
        record = self.makeRecord(self.name, level, fn, lno, msg, args,
                                 exc_info, func, extra, sinfo)

        # custom part start
        msg_labels = msg.pop('labels') if (
            isinstance(msg, Mapping) and
            'labels' in msg and
            msg['labels']) else {}
        kw_labels = kwargs['labels'] if (
            'labels' in kwargs and
            kwargs['labels']) else {}
        _labels = {**self.labels, **kw_labels, **msg_labels}

        _checkAllString(_labels)

        record.labels = _labels
        # custom part end

        self.handle(record)


class CloudLoggerManager(logging.Manager):
    """
    The object behind loggers that manage used to create loggers
    """

    def __init__(self, rootLogger) -> None:
        super().__init__(rootLogger)

        # override loggerClass to indicate how this manager make loggers
        self.setLoggerClass(CloudLogger)

    def _getBasicLogger(self, name=None, labels=None, log_level=logging.INFO):

        # Set labels to default labels if no custom labels are provided
        _labels = DEFAULT_LABELS.copy()

        if ENV_NAME__APP_ENV in os.environ:
            _labels['env'] = os.environ[ENV_NAME__APP_ENV]

        _labels.update(labels if labels else {})

        _checkAllString(_labels)

        # created logger by the name
        # or retrieve existing logger and modify it
        logger = super().getLogger(name)
        logger.setLevel(log_level)

        logger.labels = _labels

        return logger

    def getLogger(self, name=None, labels=None, log_level=logging.INFO) -> CloudLogger:
        """
        Retrieve the logger by name if it's already existed.
        Otherwise, initiate a logger object and attach default gcp handler to it, alone with the name and labels.

        This function is refence of function setup_logging from 
        https://github.com/googleapis/python-logging/blob/main/google/cloud/logging_v2/client.py

        Args:
            name (str): The name of logger, will be determined by the environment if left None.
            labels (dict): Custom labels , will be mixed with default labels, in a logger aspect.
                recommended labels:
                    env, application/project
            log_level (Optional[int]): Python logging log level. Defaults to
                :const:`logging.INFO`.
        """

        # ensure initialized
        if not isEnvironmentValid() and not isSetup():
            basicConfig()

        # get name
        _name = name if name else getDefaultLoggerName()

        # return existing logger
        if _name in self.loggerDict:
            existing_logger = self.loggerDict[_name]
            return existing_logger

        logger = self._getBasicLogger(_name, labels, log_level)

        # not propagate the logs to root logger
        if '.' not in logger.name:
            logger.propagate = False

        # set logName and labels of cloud logging by setting handler
        # https://github.com/googleapis/python-logging/blob/main/google/cloud/logging_v2/client.py#L368
        if _RESOURCE and _RESOURCE.type not in {_GAE_RESOURCE_TYPE, 'global'}:
            # TODO: to specify logName in fluentbit
            # TODO: support cloud function
            handler = StructuredLogHandler(
                project_id=_CLIENT.project, labels=logger.labels)
        else:
            handler = CloudLoggingHandler(
                _CLIENT, resource=_RESOURCE, name=logger.name)

        # remove built-in handlers on App Engine or Cloud Functions environments
        if _RESOURCE.type in _CLEAR_HANDLER_RESOURCE_TYPES:
            logger.handlers.clear()

        logger.addHandler(handler)

        for logger_name in EXCLUDED_LOGGER_DEFAULTS:
            # prevent excluded loggers from propagating logs to handler
            _logger = logging.getLogger(logger_name)
            _logger.propagate = False

        return logger

    def getPlaceHolder(self, name=None, labels=None, log_level=logging.INFO) -> CloudLogger:
        """
        Initiate a logger object without any handler attached,
        This logger is used to propagate log records only.

        Args:
            name (str): The name of logger, will be determined by the environment if left None.
            labels (dict): Custom labels , will be mixed with default labels, in a logger aspect.
                recommended labels:
                    env, application/project
            log_level (Optional[int]): Python logging log level. Defaults to
                :const:`logging.INFO`.
        """

        _name = name if name else getDefaultLoggerName()

        logger = self._getBasicLogger(_name, labels, log_level)

        logger.handlers.clear()
        logger.propagate = True

        return logger
    
    def getOfflineLogger(self, name=None, labels=None, log_level=logging.INFO) -> CloudLogger:
        """
        Initiate a logger object with only StructuredLogHandler which writes logs to console.
        This is used in fluentd environment.

        Args:
            name (str): The name of logger, will be determined by the environment if left None.
            labels (dict): Custom labels , will be mixed with default labels, in a logger aspect.
                recommended labels:
                    env, application/project
            log_level (Optional[int]): Python logging log level. Defaults to
                :const:`logging.INFO`.
        """

        _name = name if name else getDefaultLoggerName()

        logger = self._getBasicLogger(_name, labels, log_level)

        # not propagate the logs to root logger
        if '.' not in logger.name:
            logger.propagate = False

        handler = StructuredLogHandler(labels=logger.labels)
        logger.addHandler(handler)

        return logger


CloudLogger.manager = CloudLoggerManager(logging.root)
