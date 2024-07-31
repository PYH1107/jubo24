"""
This module is refence from
    https://github.com/perdy/starlette-prometheus

"""
import time
from typing import Tuple
from datetime import datetime

import jwt
import pytz
from prometheus_client import Counter, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Match
from starlette.types import ASGIApp

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

PUBLISHER = Publisher(mode='prod')
PUBSUB_TOPIC = 'api-user-logs'
# create topic
# PUBLISHER.create_topic(PUBSUB_TOPIC)


def request_info_recorder(request, status_code, request_latency):
    # parse authorization info
    token = request.headers.get('authorization', '')
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
    ip_addr = request.headers.get('x-forwarded-for', request.client[0])
    request_info = {
        "method": request.method,
        "path": request.get('path', ''),
        "query_string": request.get('query_string', b'').decode(),
        "ip_addr": ip_addr,
        "HTTP_X_FORWARDED_FOR": request.headers.get('x-forwarded-for', ''),
        "HTTP_X_ENVOY_ORIGINAL_PATH": request.headers.get('x-envoy-original-path', ''),
        "sub": auth_info["sub"],
        "iat": auth_info["iat"],
        "exp": auth_info["exp"],
        "status_code": status_code,
        "latency": request_latency,
        "create_dt": now_dt
    }
    # publishing data
    PUBLISHER.publish(PUBSUB_TOPIC, request_info)


class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, filter_unhandled_paths: bool = False) -> None:
        super().__init__(app)
        self.filter_unhandled_paths = filter_unhandled_paths

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        method = request.method
        path_template, is_handled_path = self.get_path_template(request)

        if self._is_path_filtered(is_handled_path):
            return await call_next(request)

        before_time = time.perf_counter()
        response = await call_next(request)

        status_code = response.status_code
        after_time = time.perf_counter()
        request_latency = after_time - before_time
        METRICS_REQUEST_LATENCY.labels(method, path_template).observe(
            request_latency
        )
        METRICS_REQUEST_COUNT.labels(
            method, path_template, status_code
        ).inc()

        request_info_recorder(request, status_code, request_latency)

        return response

    @staticmethod
    def get_path_template(request: Request) -> Tuple[str, bool]:
        for route in request.app.routes:
            match, child_scope = route.matches(request.scope)
            if match == Match.FULL:
                return route.path, True

        return request.url.path, False

    def _is_path_filtered(self, is_handled_path: bool) -> bool:
        return self.filter_unhandled_paths and not is_handled_path
