from warnings import warn
from functools import wraps
import pymongo

from linkinpark.lib.common.secret_accessor import SecretAccessor

ACCESSOR = None


def _init_accessor(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global ACCESSOR
        if not ACCESSOR:
            ACCESSOR = SecretAccessor()
        return func(*args, **kwargs)
    return wrapper


@_init_accessor
def MongodbAI(selection_timeout=20000, **kwargs):
    mongodbUrlAI = ACCESSOR.access_secret("mongodbUrlAI")
    myclient = pymongo.MongoClient(
        mongodbUrlAI,
        tlsAllowInvalidCertificates=True,
        authSource="admin",
        serverSelectionTimeoutMS=selection_timeout,
        **kwargs)
    return myclient['aimodel']


def MongodbNISReadOnly(selection_timeout=20000, **kwargs):
    warn("MongodbNISReadWrite is deprecated, please use MongodbReadOnly "
         "instead.",
         DeprecationWarning, stacklevel=2)
    myclient = MongodbReadOnly(
        env="prod",
        database=None,
        app_name="aids_datahub",
        selection_timeout=selection_timeout,
        **kwargs)
    return myclient


@_init_accessor
def MongodbReadOnly(env="demo", database=None, app_name="aids", selection_timeout=20000, **kwargs):
    mongodbUrlNISAnalytics = ACCESSOR.access_secret("mongodbUrlNISAnalytics")
    mongodbUrlDEMOAnalytics = ACCESSOR.access_secret("mongodbUrlDEMOAnalytics")
    mongodbUrlDEVAnalytics = ACCESSOR.access_secret("mongodbUrlDEVAnalytics")

    urls = {
        "prod": {"url": mongodbUrlNISAnalytics, "database": "release"},
        "aids": {"url": mongodbUrlDEMOAnalytics, "database": "demo"},
        "demo": {"url": mongodbUrlDEMOAnalytics, "database": "demo"},
        "dev": {"url": mongodbUrlDEVAnalytics, "database": "develop"},
    }
    client_url = urls[env]["url"] + "&appName=" + app_name
    if database:
        client_database = urls[env]["database"] + "_" + database
    else:
        client_database = urls[env]["database"]
    myclient = pymongo.MongoClient(
        client_url,
        tlsAllowInvalidCertificates=True,
        authSource="admin",
        serverSelectionTimeoutMS=selection_timeout,
        **kwargs)
    return myclient[client_database]


@_init_accessor
def MongodbReadWrite(env="demo", database=None, app_name="aids", selection_timeout=20000, **kwargs):
    mongodbUrlNIS = ACCESSOR.access_secret("mongodbUrlNIS")
    mongodbUrlDEMO = ACCESSOR.access_secret("mongodbUrlDEMO")
    mongodbUrlDEV = ACCESSOR.access_secret("mongodbUrlDEV")
    urls = {
        "prod": {"url": mongodbUrlNIS, "database": "release"},
        "aids": {"url": mongodbUrlDEMO, "database": "demo"},
        "demo": {"url": mongodbUrlDEMO, "database": "demo"},
        "dev": {"url": mongodbUrlDEV, "database": "develop"},
    }
    client_url = urls[env]["url"] + "&appName=" + app_name
    if database:
        client_database = urls[env]["database"] + "_" + database
    else:
        client_database = urls[env]["database"]
    myclient = pymongo.MongoClient(
        client_url,
        tlsAllowInvalidCertificates=True,
        authSource="admin",
        serverSelectionTimeoutMS=selection_timeout,
        **kwargs)
    return myclient[client_database]
