import logging
from logging.config import dictConfig

_loggers = {}


def get_logger(name):
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(levelprefix)s %(asctime)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",

            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
        },
        "loggers": {
            "ai-vitalsign": {"handlers": ["default"], "level": "DEBUG"},
        },
    }

    dictConfig(log_config)
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('ai-vitalsign')
    logger.setLevel(logging.INFO)
    _loggers[name] = logger
    return logger


logger = get_logger('vitalsign')
