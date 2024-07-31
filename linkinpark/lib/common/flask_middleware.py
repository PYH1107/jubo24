import os
import json
import psutil
from psutil._common import bytes2human
from werkzeug.middleware.dispatcher import DispatcherMiddleware


class FlaskMiddleware:
    def __init__(self, app, path_prefix="/predict"):
        self.app = app
        self.path_prefix = path_prefix
        self.app.wsgi_app = DispatcherMiddleware(
            self.app.wsgi_app, {
                f"{self.path_prefix}/health-check": self.health_check,
                f"{self.path_prefix}/resc-usage": self.resource_usage,
            }
        )

    def health_check(self, environ, start_response):
        app_version = os.environ.get("APP_VERSION")
        response_body = json.dumps({'app_version': app_version})
        headers = [('Content-Type', 'application/json')]
        start_response('200 OK', headers)
        return [response_body.encode('utf-8')]

    def resource_usage(self, environ, start_response):
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()

        # Get RAM usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_used = bytes2human(memory.used)

        usage = json.dumps({
            "cpu_percent": cpu_percent,
            "cpu_count": cpu_count,
            "memory_percent": memory_percent,
            "memory_used": memory_used,
        })
        start_response('200 OK', [('Content-Type', 'application/json')])
        return [usage.encode('utf-8')]
