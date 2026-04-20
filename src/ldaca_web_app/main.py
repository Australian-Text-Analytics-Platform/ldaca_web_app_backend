"""
Enhanced LDaCA Web App API - Main FastAPI Application
Modular, production-ready text analysis platform with multi-user support
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import IO, Any, cast

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# Import API routers
from .api.admin import router as admin_router
from .api.auth import router as auth_router
from .api.config import router as config_router
from .api.files import router as files_router
from .api.preferences import router as preferences_router
from .api.tasks import router as tasks_router
from .api.text import router as text_router
from .api.workspaces import router as workspaces_router

# Ensure DocWorkspace API conversion utilities are available at startup.
from .core import docworkspace_data_types  # noqa: F401
from .db import cleanup_expired_sessions, init_db
from .settings import reload_settings, settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup/shutdown lifecycle for backend runtime dependencies.

    Used by:
    - FastAPI app lifecycle hooks

    Why:
    - Initializes data folders/DB/session cleanup and performs safe worker shutdown.
    """
    # Setup file logging for packaged app (especially Windows)
    from ._logging import setup_file_logging, setup_logging

    setup_logging()
    log_file: IO[str] | None = setup_file_logging("main")
    current_settings = reload_settings()

    # Startup
    logger.info("=" * 60)
    logger.info("Starting LDaCA Web App...")
    logger.info("Platform: %s", sys.platform)
    logger.info("Python version: %s", sys.version)
    logger.info("=" * 60)

    logger.info("Step 1: Preparing runtime")
    logger.info("Step 1 complete")

    # Ensure DATA_ROOT and data folders exist before DB init
    logger.info("Step 2: Creating data folders")
    current_settings.get_data_root().mkdir(parents=True, exist_ok=True)
    current_settings.get_user_data_folder().mkdir(parents=True, exist_ok=True)
    sample_override = current_settings.get_sample_data_folder()
    if sample_override:
        sample_override.mkdir(parents=True, exist_ok=True)
        logger.info("Sample data folder: %s", sample_override)
    else:
        logger.info("Sample data folder: packaged resources")
    current_settings.get_database_backup_folder().mkdir(parents=True, exist_ok=True)
    logger.info("Step 2 complete")

    # Initialize database
    logger.info("Step 3: Initializing database")
    await init_db()
    logger.info("Step 3a: Database initialized")
    await cleanup_expired_sessions()
    logger.info("Step 3 complete")

    # Worker pool will start lazily on first task submission
    logger.info("Step 4: Configuring worker pool")
    logger.info("Worker pool configured for lazy initialization")
    logger.info("Step 4 complete")

    # Prefetch heavy ML models in a background thread
    logger.info("Step 5: Starting background model prefetch")
    from .core.model_prefetch import start_model_prefetch

    start_model_prefetch()
    logger.info("Step 5 complete (downloads continue in background)")

    logger.info("=" * 60)
    logger.info("SUCCESS: Backend startup complete!")

    logger.info(
        "API Documentation: http://%s:%s/api/docs",
        current_settings.server_host,
        current_settings.backend_port,
    )
    logger.info(
        "Health Check: http://%s:%s/health",
        current_settings.server_host,
        current_settings.backend_port,
    )
    logger.info("=" * 60)

    yield  # Application runs here

    # Shutdown
    logger.info("Shutting down Enhanced LDaCA Web App API...")

    # Close log file if it was opened
    if log_file:
        try:
            log_file.close()
        except Exception:
            pass

    # Shutdown worker pool with timeout to prevent hanging
    try:
        from .core.worker import get_worker_pool

        worker_pool = get_worker_pool()
        if worker_pool.is_running:
            logger.info("Shutting down worker pool...")
            worker_pool.shutdown(wait=True, timeout=5.0)
            logger.info("Worker pool shutdown complete")
    except Exception as e:
        logger.warning("Error during worker pool shutdown: %s", e)

    await cleanup_expired_sessions()


# Create FastAPI application
app = FastAPI(
    title="Enhanced LDaCA Web App API",
    version="3.0.0",
    description="Multi-user text analysis platform with workspace management and polars-text integration",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# Setup CORS (regex + credentials from settings)
# Allow:
# - http://localhost:* and http://127.0.0.1:* for web dev/production
# - tauri://localhost and https://tauri.localhost for Tauri desktop app (v1 and v2)
# - Allow all origins via regex to ensure no blocking on desktop
app.add_middleware(
    cast(Any, CORSMiddleware),
    allow_origin_regex=r".*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers with /api prefix
app.include_router(auth_router, prefix="/api", tags=["authentication"])
app.include_router(config_router, prefix="/api", tags=["configuration"])
app.include_router(files_router, prefix="/api", tags=["file_management"])
app.include_router(preferences_router, prefix="/api", tags=["preferences"])
app.include_router(tasks_router, prefix="/api", tags=["task_streaming"])
app.include_router(text_router, prefix="/api", tags=["text_analysis"])
app.include_router(workspaces_router, prefix="/api", tags=["workspace_management"])
app.include_router(admin_router, prefix="/api", tags=["administration"])


# =============================================================================
# ROOT ENDPOINTS
# =============================================================================


@app.get("/")
async def root():
    """Return API feature/index metadata.

    Used by:
    - browser/manual root checks and basic service discovery

    Why:
    - Provides a human-readable entrypoint summary for backend capabilities.
    """
    return {
        "message": "Enhanced LDaCA Web App API",
        "version": "3.0.0",
        "description": "Multi-user text analysis platform with workspace management",
        "features": {
            "authentication": "Google OAuth 2.0",
            "workspaces": "Multi-user workspace management with node operations",
            "file_management": "Upload, preview, download with type detection",
            "text_analysis": "polars-text integration",
            "data_operations": "Filter, slice, transform, aggregate operations",
            "user_isolation": "Per-user data folders and workspace separation",
        },
        "endpoints": {
            "docs": "/api/docs",
            "redoc": "/api/redoc",
            "openapi": "/api/openapi.json",
            "health": "/health",
            "status": "/status",
            "auth": {
                "google": "/api/auth/google",
                "me": "/api/auth/me",
                "logout": "/api/auth/logout",
                "status": "/api/auth/status",
            },
            "files": {
                "list": "/api/files/",
                "upload": "/api/files/upload",
                "download": "/api/files/{filename}",
                "preview": "/api/files/preview",
                "info": "/api/files/{filename}/info",
                "delete": "/api/files/{filename}",
            },
            "workspaces": {
                "create": "/api/workspaces/",
                "current": "/api/workspaces/current",
                "info": "/api/workspaces/info",
                "delete": "/api/workspaces/delete",
                "nodes": "/api/workspaces/nodes",
                "node_data": "/api/workspaces/nodes/{node_id}/data",
                "save": "/api/workspaces/save",
                "description": "/api/workspaces/description",
                "unload": "/api/workspaces/unload",
            },
            "admin": {"users": "/api/admin/users", "cleanup": "/api/admin/cleanup"},
        },
    }


@app.get("/health")
async def health_check():
    """Return lightweight health status for liveness probes.

    Used by:
    - deployment health checks and uptime monitors

    Why:
    - Gives a fast no-auth probe endpoint for runtime readiness checks.
    """
    return {
        "status": "healthy",
        "version": "3.0.0",
        "system": "Enhanced LDaCA Web App API",
        "database": "connected",
        "features": {
            "polars-text": True,
            "docworkspace": True,
        },
        "config": {
            "data_folder": str(settings.get_data_root()),
            "debug_mode": settings.debug,
        },
    }


@app.get("/status")
async def status():
    """Return detailed component/module status information.

    Used by:
    - diagnostics pages and manual troubleshooting

    Why:
    - Exposes richer operational metadata than `/health`.
    """
    return {
        "system": "Enhanced LDaCA Web App API",
        "version": "3.0.0",
        "status": "operational",
        "components": {
            "authentication": {
                "status": "[OK] Google OAuth 2.0",
                "description": "Secure user authentication with session management",
            },
            "file_management": {
                "status": "[OK] Multi-format support",
                "description": "Upload, download, preview CSV, JSON, Parquet, Excel files",
            },
            "workspace_management": {
                "status": "[OK] Multi-user isolation",
                "description": "Per-user workspaces with DataFrame node operations",
            },
            "data_operations": {
                "status": "[OK] DataFrame manipulation",
                "description": "Filter, slice, transform, aggregate, join operations",
            },
            "text_analysis": {
                "status": "[OK] polars-text ready",
                "description": "Advanced text analysis with polars-text integration",
            },
            "database": {
                "status": "[OK] SQLAlchemy async",
                "description": "Async SQLAlchemy with session management",
            },
        },
        "modules": {
            "auth": "Google OAuth authentication and session management",
            "files": "File upload, download, preview, and management",
            "workspaces": "Multi-user workspace and node management",
            "admin": "Administrative functions and monitoring",
        },
    }


# ---------------------------------------------------------------------------
# Unified server launcher
# ---------------------------------------------------------------------------

_server: uvicorn.Server | None = None
_server_task: asyncio.Task[None] | None = None


def _clear_server_state(_task: asyncio.Task[None] | None = None) -> None:
    """Reset cached server state after a background task finishes."""
    global _server, _server_task
    _server = None
    _server_task = None


def _get_frontend_build_dir():
    """Locate the bundled frontend build directory using importlib.resources."""
    from importlib import resources
    from pathlib import Path

    pkg = resources.files("ldaca_web_app.resources.frontend")
    build_dir = Path(str(pkg / "build"))
    if not build_dir.is_dir():
        logger.error("Frontend build not found at %s", build_dir)
        logger.error(
            "Run 'npm run build -w frontend' and copy build/ to "
            "backend/src/ldaca_web_app/resources/frontend/build/"
        )
        raise FileNotFoundError(f"Frontend build not found at {build_dir}")
    return build_dir


def _mount_frontend(target_app: FastAPI) -> None:
    """Mount the bundled frontend SPA onto *target_app*.

    Injects ``window.__BASE_PATH__`` into ``index.html`` so the frontend
    discovers its base path and the API endpoint regardless of deployment
    scenario (local, server, Binder/JupyterHub proxy, Tauri, etc.).

    The base path comes from the ASGI ``root_path`` which is set by
    reverse proxies or explicitly via ``--root-path`` in uvicorn.
    """
    from starlette.responses import FileResponse, HTMLResponse
    from starlette.staticfiles import StaticFiles

    build_dir = _get_frontend_build_dir()
    index_html = build_dir / "index.html"
    _raw_index_html = index_html.read_text()

    def _inject_base_path(html: str, base_path: str) -> str:
        """Inject runtime globals into the HTML <head>."""
        import json

        globals_payload = {
            "__BASE_PATH__": base_path,
            "__GOOGLE_CLIENT_ID__": settings.google_client_id or "",
            "__MULTI_USER__": settings.multi_user,
        }
        assignments = ";".join(
            f"window.{key}={json.dumps(value)}"
            for key, value in globals_payload.items()
        )
        script = f"<script>{assignments};</script>"
        return html.replace("<head>", f"<head>{script}", 1)

    # Serve static asset subdirectories (JS/CSS/images)
    for subdir in build_dir.iterdir():
        if subdir.is_dir():
            target_app.mount(
                f"/{subdir.name}",
                StaticFiles(directory=str(subdir)),
                name=f"frontend-{subdir.name}",
            )

    # Replace the default JSON root endpoint with the frontend SPA handler
    target_app.routes[:] = [
        r
        for r in target_app.routes
        if not (
            hasattr(r, "path")
            and r.path == "/"
            and hasattr(r, "methods")
            and "GET" in getattr(r, "methods", set())
        )
    ]

    @target_app.get("/")
    async def _serve_index(request: Request):
        base_path = (request.scope.get("root_path") or "").rstrip("/")
        return HTMLResponse(_inject_base_path(_raw_index_html, base_path))

    @target_app.get("/{path:path}")
    async def _serve_frontend(path: str, request: Request):
        file_path = build_dir / path
        if path and file_path.is_file():
            return FileResponse(str(file_path))
        base_path = (request.scope.get("root_path") or "").rstrip("/")
        return HTMLResponse(_inject_base_path(_raw_index_html, base_path))


def _create_frontend_only_app(port: int) -> FastAPI:
    """Build a minimal FastAPI app that only serves the frontend SPA."""
    frontend_app = FastAPI(title="LDaCA Frontend", docs_url=None, redoc_url=None)
    _mount_frontend(frontend_app)
    return frontend_app


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
    """
    if not backend and not frontend:
        raise ValueError("At least one of backend or frontend must be True")

    global _server, _server_task
    global settings

    _port = port or (8001 if backend else 3000)
    _host = host or ("localhost" if background else "0.0.0.0")

    # Write effective values into env vars, then reload the settings singleton
    # so every consumer reads consistent, up-to-date config.
    os.environ["BACKEND_PORT"] = str(_port)
    os.environ["SERVER_HOST"] = _host

    current = reload_settings()
    settings = current

    # Auto-detect root_path from JupyterHub/Binder environment
    _root_path = root_path
    if _root_path is None:
        hub_prefix = os.environ.get("JUPYTERHUB_SERVICE_PREFIX", "")
        if hub_prefix:
            _root_path = f"{hub_prefix.rstrip('/')}/proxy/{_port}"

    # Choose which app to serve
    if backend:
        target_app = app
        if frontend:
            _mount_frontend(target_app)
    else:
        target_app = _create_frontend_only_app(_port)

    if background:
        # Idempotent: reuse existing task if still running
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

    # Blocking mode
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
