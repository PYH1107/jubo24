import os
import psutil
from psutil._common import bytes2human
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


class FastAPIMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, path_prefix="/predict"):
        super().__init__(app)
        self.path_prefix = path_prefix

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path == f"{self.path_prefix}/health-check":
            return await self.health_check(request)
        elif path == f"{self.path_prefix}/resc-usage":
            return await self.resc_usage(request)
        return await call_next(request)

    async def health_check(self, request: Request):
        app_version = os.environ.get("APP_VERSION")
        return JSONResponse({"app_version": app_version})

    async def resc_usage(self, request: Request):
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()

        # Get RAM usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_used = bytes2human(memory.used)

        return JSONResponse({
            "cpu_percent": cpu_percent,
            "cpu_count": cpu_count,
            "memory_percent": memory_percent,
            "memory_used": memory_used,
        })
