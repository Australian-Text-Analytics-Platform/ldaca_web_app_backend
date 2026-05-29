"""Unified server launcher for the LDaCA backend.

Used by:
- desktop/runtime launchers, Jupyter/Colab notebooks, and ``__init__.py``
  because they need a single entry point for starting the server in
  blocking or background mode.

Flow: resolve host/port/root_path from env and settings, select the
    appropriate FastAPI app (full backend, backend+SPA, or SPA-only),
    and launch uvicorn either as a non-blocking task or in blocking mode.
"""

import asyncio
import logging
import os
import sys

import uvicorn
from fastapi import FastAPI

from .main import __version__, app
from .settings import reload_settings, settings
from .spa import _create_frontend_only_app, _mount_frontend

logger = logging.getLogger(__name__)

_server: uvicorn.Server | None = None
_server_task: asyncio.Task[None] | None = None


def _clear_server_state(_task: asyncio.Task[None] | None = None) -> None:
    """Reset cached server state after a background task finishes.

    Called by:
    - ``start_server`` as a done callback on the background server task,
      so that a subsequent call can create a fresh server.
    """
    global _server, _server_task
    _server = None
    _server_task = None


def start_server(
    *,
    backend: bool = True,
    frontend: bool = True,
    port: int | None = None,
    host: str | None = None,
    background: bool = False,
    root_path: str | None = None,
) -> asyncio.Task[None] | None:
    """Unified entry point for launching the LDaCA server.

    Args:
        backend: Include the full API backend (routers, lifespan, DB, etc.).
        frontend: Mount the bundled frontend SPA on the same server.
        port: Port to bind to. Defaults to 8001 (backend) or 3000 (frontend-only).
        host: Host to bind to. Defaults to ``"localhost"`` when *background* is
            ``True``, ``"0.0.0.0"`` otherwise.
        background: When ``True``, start the server as a non-blocking
            ``asyncio.Task`` (for notebook / Colab usage) and return the task.
            When ``False`` (default), block with ``uvicorn.run()``.
        root_path: ASGI root path prefix, used when behind a reverse proxy.
            Auto-detected from ``JUPYTERHUB_SERVICE_PREFIX`` + ``proxy/<port>``
            if not provided and running inside JupyterHub/Binder.

    Returns:
        The ``asyncio.Task`` when *background* is ``True``, otherwise ``None``
        (blocks until the server shuts down).

    Raises:
        ValueError: If both *backend* and *frontend* are ``False``.

    Used by:
    - FastAPI application startup, backend package imports because they need a backend
      boundary that validates inputs before delegating to workspace or worker state.
    """
    if not backend and not frontend:
        raise ValueError("At least one of backend or frontend must be True")

    global _server, _server_task
    global settings

    _env_port = os.environ.get("LDACA_BACKEND_PORT") or os.environ.get("BACKEND_PORT")
    _port = port or (int(_env_port) if _env_port else (8001 if backend else 3000))
    _host = host or ("localhost" if background else "0.0.0.0")

    os.environ["BACKEND_PORT"] = str(_port)
    os.environ["SERVER_HOST"] = _host

    current = reload_settings()
    settings = current

    _root_path = root_path
    if _root_path is None:
        hub_prefix = os.environ.get("JUPYTERHUB_SERVICE_PREFIX", "")
        if hub_prefix:
            _root_path = f"{hub_prefix.rstrip('/')}/proxy/{_port}"

    if backend:
        target_app = app
        if frontend:
            _mount_frontend(target_app)
    else:
        target_app = _create_frontend_only_app(_port)

    if background:
        if _server_task is not None:
            if _server_task.done():
                _clear_server_state()
            else:
                logger.info(
                    "Server already running at http://localhost:%s",
                    current.backend_port,
                )
                return _server_task

        config = uvicorn.Config(
            target_app,
            host=current.server_host,
            port=current.backend_port,
            root_path=_root_path or "",
            reload=False,
            log_level="info",
        )
        _server = uvicorn.Server(config)
        loop = asyncio.get_running_loop()
        _server_task = loop.create_task(_server.serve())
        _server_task.add_done_callback(_clear_server_state)
        return _server_task

    is_frozen = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")
    use_reload = current.debug and not is_frozen

    uvicorn.run(
        target_app,
        host=current.server_host,
        port=current.backend_port,
        root_path=_root_path or "",
        reload=use_reload,
        log_level="info",
    )
    return None
