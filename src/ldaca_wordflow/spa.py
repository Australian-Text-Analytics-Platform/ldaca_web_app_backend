"""SPA frontend mounting helpers for the FastAPI application.

Used by:
- ``main.py`` and ``server_launcher.py`` for serving the bundled React frontend
  alongside the API backend.

Flow: locate the frontend build directory, mount static files, and inject
    runtime configuration (base path, auth client IDs) into index.html.
"""

import logging
from pathlib import Path

from fastapi import FastAPI, Request

from .settings import settings

logger = logging.getLogger(__name__)

STATIC_DIR = "static"
INDEX_HTML_PATH = "index.html"


def _get_frontend_build_dir() -> Path:
    """Locate the bundled frontend build directory using importlib.resources.

    Called by:
    - ``_mount_frontend`` because it needs the build directory to serve
      assets and index.html.
    """
    from importlib import resources

    pkg = resources.files("ldaca_wordflow.resources.frontend")
    build_dir = Path(str(pkg / "build"))
    if not build_dir.is_dir():
        logger.error("Frontend build not found at %s", build_dir)
        logger.error(
            "Run 'npm run build -w frontend' and copy build/ to "
            "backend/src/ldaca_wordflow/resources/frontend/build/"
        )
        raise FileNotFoundError(f"Frontend build not found at {build_dir}")
    return build_dir


def _inject_base_path(html: str, base_path: str) -> str:
    """Inject runtime globals into the HTML <head>.

    Called by:
    - The ``_mount_frontend`` workflow because the SPA needs to discover its
      base path, Google client ID, and other env-specific settings at boot.
    """
    import json

    globals_payload = {
        "__BASE_PATH__": base_path,
        "__GOOGLE_CLIENT_ID__": settings.google_client_id or "",
        "__MULTI_USER__": settings.multi_user,
        "__CILOGON_CLIENT_ID__": settings.cilogon_client_id or "",
    }
    assignments = ";".join(
        f"window.{key}={json.dumps(value)}"
        for key, value in globals_payload.items()
    )
    script = f"<script>{assignments};</script>"
    return html.replace("<head>", f"<head>{script}", 1)


def _mount_frontend(target_app: FastAPI) -> None:
    """Mount the bundled frontend SPA onto *target_app*.

    Injects ``window.__BASE_PATH__`` into ``index.html`` so the frontend
    discovers its base path and the API endpoint regardless of deployment
    scenario (local, server, Binder/JupyterHub proxy, Tauri, etc.).

    The base path comes from the ASGI ``root_path`` which is set by
    reverse proxies or explicitly via ``--root-path`` in uvicorn.

    Called by:
    - ``start_server`` and ``_create_frontend_only_app`` because they need
      the SPA served alongside (or instead of) the API.
    """
    from starlette.responses import FileResponse, HTMLResponse
    from starlette.staticfiles import StaticFiles

    build_dir = _get_frontend_build_dir()
    index_html = build_dir / "index.html"
    _raw_index_html = index_html.read_text()

    for subdir in build_dir.iterdir():
        if subdir.is_dir():
            target_app.mount(
                f"/{subdir.name}",
                StaticFiles(directory=str(subdir)),
                name=f"frontend-{subdir.name}",
            )

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
        """Serve the SPA index.html at the root path.

        Called by:
        - FastAPI when matching GET / after the frontend is mounted.
        """
        base_path = (request.scope.get("root_path") or "").rstrip("/")
        return HTMLResponse(_inject_base_path(_raw_index_html, base_path))

    @target_app.get("/{path:path}")
    async def _serve_frontend(path: str, request: Request):
        """Serve frontend static assets or fall back to index.html.

        Called by:
        - FastAPI when matching any unmatched path after the frontend is mounted.
        """
        file_path = build_dir / path
        if path and file_path.is_file():
            return FileResponse(str(file_path))
        base_path = (request.scope.get("root_path") or "").rstrip("/")
        return HTMLResponse(_inject_base_path(_raw_index_html, base_path))


def _create_frontend_only_app(port: int) -> FastAPI:
    """Build a minimal FastAPI app that only serves the frontend SPA.

    Called by:
    - ``start_server`` when ``backend=False``, so the server only hosts
      the frontend without API routes.
    """
    frontend_app = FastAPI(title="LDaCA Frontend", docs_url=None, redoc_url=None)
    _mount_frontend(frontend_app)
    return frontend_app
