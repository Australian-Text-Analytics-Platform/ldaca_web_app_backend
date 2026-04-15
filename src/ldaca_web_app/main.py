"""
Enhanced LDaCA Web App API - Main FastAPI Application
Modular, production-ready text analysis platform with multi-user support
"""

from __future__ import annotations

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from typing import IO, Any, cast

import uvicorn
from fastapi import FastAPI
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
from .settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup/shutdown lifecycle for backend runtime dependencies.

    Used by:
    - FastAPI app lifecycle hooks

    Why:
    - Initializes data folders/DB/session cleanup and performs safe worker shutdown.
    """
    # Setup file logging for packaged app (especially Windows)
    from ._logging import setup_file_logging

    log_file: IO[str] | None = setup_file_logging("main")

    # Startup
    print("=" * 70, flush=True)
    print("[main] Starting LDaCA Web App...", flush=True)
    print(f"[main] Platform: {sys.platform}", flush=True)
    print(f"[main] Python version: {sys.version}", flush=True)
    print("=" * 70, flush=True)

    print("[main] Step 1: Preparing runtime", flush=True)
    print("[main] Step 1 complete", flush=True)

    # Ensure DATA_ROOT and data folders exist before DB init
    print("[main] Step 2: Creating data folders", flush=True)
    settings.get_data_root().mkdir(parents=True, exist_ok=True)
    settings.get_user_data_folder().mkdir(parents=True, exist_ok=True)
    sample_override = settings.get_sample_data_folder()
    if sample_override:
        sample_override.mkdir(parents=True, exist_ok=True)
        print(f"[main] Sample data folder: {sample_override}", flush=True)
    else:
        print("[main] Sample data folder: packaged resources", flush=True)
    settings.get_database_backup_folder().mkdir(parents=True, exist_ok=True)
    print("[main] Step 2 complete", flush=True)

    # Initialize database
    print("[main] Step 3: Initializing database", flush=True)
    await init_db()
    print("[main] Step 3a: Database initialized", flush=True)
    await cleanup_expired_sessions()
    print("[main] Step 3 complete", flush=True)

    # Worker pool will start lazily on first task submission
    print("[main] Step 4: Configuring worker pool", flush=True)
    print("[main] Worker pool configured for lazy initialization", flush=True)
    print("[main] Step 4 complete", flush=True)

    # Prefetch heavy ML models in a background thread
    print("[main] Step 5: Starting background model prefetch", flush=True)
    from .core.model_prefetch import start_model_prefetch

    start_model_prefetch()
    print("[main] Step 5 complete (downloads continue in background)", flush=True)

    print("=" * 70, flush=True)
    print("[main] SUCCESS: Backend startup complete!", flush=True)

    print(
        f"[main] API Documentation: http://{settings.server_host}:{settings.backend_port}/api/docs"
    )
    print(
        f"[main] Health Check: http://{settings.server_host}:{settings.backend_port}/health",
        flush=True,
    )
    print("=" * 70, flush=True)

    yield  # Application runs here

    # Shutdown
    print("[main] Shutting down Enhanced LDaCA Web App API...", flush=True)

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
            print("Shutting down worker pool...")
            worker_pool.shutdown(wait=True, timeout=5.0)
            print("Worker pool shutdown complete")
    except Exception as e:
        print(f"Warning: Error during worker pool shutdown: {e}")

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
        print(f"[ldaca] ERROR: Frontend build not found at {build_dir}", flush=True)
        print("[ldaca] Run 'npm run build -w frontend' and copy build/ to", flush=True)
        print(
            "[ldaca]   backend/src/ldaca_web_app/resources/frontend/build/", flush=True
        )
        raise FileNotFoundError(f"Frontend build not found at {build_dir}")
    return build_dir


def _mount_frontend(target_app: FastAPI, port: int) -> None:
    """Mount the bundled frontend SPA onto *target_app*.

    Injects ``window.__BACKEND_URL__`` into ``index.html`` so the frontend
    discovers the API on the same origin.
    """
    from starlette.responses import FileResponse, HTMLResponse
    from starlette.staticfiles import StaticFiles

    build_dir = _get_frontend_build_dir()
    index_html = build_dir / "index.html"

    _index_html_text = index_html.read_text()
    _backend_url_script = (
        f'<script>window.__BACKEND_URL__="http://localhost:{port}";</script>'
    )
    _index_html_text = _index_html_text.replace(
        "<head>", f"<head>{_backend_url_script}", 1
    )

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
    async def _serve_index():
        return HTMLResponse(_index_html_text)

    @target_app.get("/{path:path}")
    async def _serve_frontend(path: str):
        file_path = build_dir / path
        if path and file_path.is_file():
            return FileResponse(str(file_path))
        return HTMLResponse(_index_html_text)


def _create_frontend_only_app(port: int) -> FastAPI:
    """Build a minimal FastAPI app that only serves the frontend SPA."""
    frontend_app = FastAPI(title="LDaCA Frontend", docs_url=None, redoc_url=None)
    _mount_frontend(frontend_app, port)
    return frontend_app


def start_server(
    *,
    backend: bool = True,
    frontend: bool = True,
    port: int | None = None,
    host: str | None = None,
    background: bool = False,
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

    Returns:
        The ``asyncio.Task`` when *background* is ``True``, otherwise ``None``
        (blocks until the server shuts down).

    Raises:
        ValueError: If both *backend* and *frontend* are ``False``.
    """
    if not backend and not frontend:
        raise ValueError("At least one of backend or frontend must be True")

    global _server, _server_task

    _port = port or (8001 if backend else 3000)
    _host = host or ("localhost" if background else "0.0.0.0")

    # Write effective values into env vars, then reload the settings singleton
    # so every consumer reads consistent, up-to-date config.
    os.environ["BACKEND_PORT"] = str(_port)
    os.environ["SERVER_HOST"] = _host

    from .settings import reload_settings

    current = reload_settings()

    # Choose which app to serve
    if backend:
        target_app = app
        if frontend:
            _mount_frontend(target_app, _port)
    else:
        target_app = _create_frontend_only_app(_port)

    if background:
        # Idempotent: reuse existing task if still running
        if _server_task is not None:
            if _server_task.done():
                _clear_server_state()
            else:
                print(
                    f"Server already running at http://localhost:{current.backend_port}",
                    flush=True,
                )
                return _server_task

        config = uvicorn.Config(
            target_app,
            host=current.server_host,
            port=current.backend_port,
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
        reload=use_reload,
        log_level="info",
    )
    return None
