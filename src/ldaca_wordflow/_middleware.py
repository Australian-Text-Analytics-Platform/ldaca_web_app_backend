"""ASGI request-logging middleware.

Used by:
- ``main.py`` application startup.

Why:
- Logs every request with method, path, status, and wall-clock duration as
  structured JSON so operators can diagnose latency and errors without
  instrumenting individual route handlers.

Flow:
- Wrap ``app`` in ``RequestLoggingMiddleware``.
- On each request: capture start time, delegate to downstream ASGI app,
  capture end time, log the outcome at the appropriate level (INFO for 2xx/3xx,
  WARNING for 4xx, ERROR for 5xx).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Callable, MutableMapping

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger("ldaca_wordflow.request")


class RequestLoggingMiddleware:
    """Log HTTP method, path, status, and duration for every request."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status_code: int = 500

        async def send_wrapper(message: MutableMapping[str, Any]) -> None:
            nonlocal status_code
            if message.get("type") == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            status_code = 500
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            log_fn: Callable[..., None]
            if status_code >= 500:
                log_fn = logger.error
            elif status_code >= 400:
                log_fn = logger.warning
            else:
                log_fn = logger.info
            log_fn(
                "%s %s → %s (%.1fms)",
                scope.get("method", "?"),
                scope.get("path", "?"),
                status_code,
                duration_ms,
            )
