"""
This module is refence from
    https://github.com/pilosus/flask_prometheus_metrics

"""
import os
import time
from datetime import datetime
from importlib.resources import path

import jwt
import pytz
from flask import request
from prometheus_client import make_wsgi_app
from werkzeug.serving import run_simple
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import Counter, Histogram, Info

from linkinpark.lib.common.pubsub_manager import Publisher

METRICS_REQUEST_LATENCY = Histogram(
    "app_request_latency_seconds", "Application Request Latency", [
        "method", "endpoint"]
)

METRICS_REQUEST_COUNT = Counter(
    "app_request_count",
    "Application Request Count",
    ["method", "endpoint", "http_status"],
)

METRICS_INFO = Info("app_version", "Application Version")
APP_VERSION = os.environ.get("APP_VERSION", "Default Version")
METRICS_INFO.info({"version": APP_VERSION})

PUBLISHER = Publisher(mode='prod')
PUBSUB_TOPIC = 'api-user-logs'
# create topic
# PUBLISHER.create_topic(PUBSUB_TOPIC)


def request_info_recorder(status_code, request_latency):
    # parse authorization info
    token = request.headers.get('Authorization', '')
    if token:
        token = token.split('Bearer')[-1]
        decoded_dict = jwt.decode(token, options={"verify_signature": False})
        auth_info = {
            "sub": decoded_dict['sub'].split('|')[-1],
            "iat": decoded_dict['iat'],
            "exp": decoded_dict['exp'],
        }
    else:
        auth_info = {"sub": "", "iat": "", "exp": ""}

    now_dt = datetime.now(pytz.timezone("Asia/Taipei"))
    now_dt = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    # parse request info
    ip_addr = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
    request_info = {
        "method": request.method,
        "path": request.path,
        "query_string": request.query_string.decode(),
        "ip_addr": ip_addr,
        "HTTP_X_FORWARDED_FOR": request.environ.get('HTTP_X_FORWARDED_FOR', ''),
        "HTTP_X_ENVOY_ORIGINAL_PATH": request.environ.get('HTTP_X_ENVOY_ORIGINAL_PATH', ''),
        "sub": auth_info["sub"],
        "iat": auth_info["iat"],
        "exp": auth_info["exp"],
        "status_code": status_code,
        "latency": request_latency,
        "create_dt": now_dt
    }
    # publishing data
    PUBLISHER.publish(PUBSUB_TOPIC, request_info)


def before_request():
    """
    Get start time of a request
    """
    request._prometheus_metrics_request_start_time = time.time()


def after_request(response):
    """
    Register Prometheus metrics after each request
    """
    request_latency = time.time() - request._prometheus_metrics_request_start_time
    METRICS_REQUEST_LATENCY.labels(request.method, request.path).observe(
        request_latency
    )
    METRICS_REQUEST_COUNT.labels(
        request.method, request.path, response.status_code
    ).inc()

    request_info_recorder(response.status_code, request_latency)
    return response


class FlaskMonitorServing:
    def __init__(self, app):
        self.app = self._register_metrics(app)
        # create dispatcher, used to add /metrics
        self.app.wsgi_app = DispatcherMiddleware(
            self.app.wsgi_app, {"/metrics": make_wsgi_app()})

    def _register_metrics(self, app):
        """
        Register metrics middlewares

        Use in your application factory (i.e. create_app):
        register_middlewares(app, settings["version"], settings["config"])

        Flask application can register more than one before_request/after_request.
        Beware! Before/after request callback stored internally in a dictionary.
        Before CPython 3.6 dictionaries didn't guarantee keys order, so callbacks
        could be executed in arbitrary order.
        """
        app.before_request(before_request)
        app.after_request(after_request)
        return app

    @property
    def flask_app(self):
        return self.app

    def run(self,
            port=5000,
            use_reloader=False,
            use_debugger=True,
            use_evalex=True):
        run_simple(
            "0.0.0.0",
            port,
            self.app,
            use_reloader=use_reloader,
            use_debugger=use_debugger,
            use_evalex=use_evalex,
        )
