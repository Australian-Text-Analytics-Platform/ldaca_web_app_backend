"""
Command-line interface for LDaCA Web App.

Usage:
    uvx ldaca-web-app                  # Launch full app (backend + frontend)
    uvx ldaca-web-app --backend        # Launch backend only
    uvx ldaca-web-app --frontend       # Launch frontend only (requires built assets)
    uvx ldaca-web-app --port 9000      # Custom port
"""

import atexit
import signal
import sys


def _parse_args(argv: list[str] | None = None):
    import argparse

    parser = argparse.ArgumentParser(
        prog="ldaca-web-app",
        description="LDaCA Text Analytics Web Application",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--backend",
        action="store_true",
        help="Launch only the backend server",
    )
    group.add_argument(
        "--frontend",
        action="store_true",
        help="Launch only the frontend server (requires built assets)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port to serve on (default: 8001 for backend, 3000 for frontend)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Host to bind to (default: 0.0.0.0)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    """CLI entry point dispatching to backend, frontend, or both."""
    args = _parse_args(argv)

    if args.frontend:
        _run_frontend(port=args.port, host=args.host)
    elif args.backend:
        _run_backend(port=args.port, host=args.host)
    else:
        # Full app: launch both backend and frontend
        _run_all(port=args.port, host=args.host)


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
        sys.exit(1)
    return build_dir


def _run_all(*, port: int | None = None, host: str | None = None):
    """Launch backend with frontend assets served on the same origin.

    Mounts the built frontend as static files on the FastAPI app so the
    frontend's same-origin fallback (``${origin}/api``) works automatically.
    """
    _port = port or 8001
    _host = host or "0.0.0.0"

    # Set env vars BEFORE any ldaca_web_app import so pydantic-settings picks them up.
    # _get_frontend_build_dir() triggers ldaca_web_app.__init__ → settings import.
    import os

    os.environ["BACKEND_PORT"] = str(_port)
    os.environ["LDACA_BACKEND_PORT"] = str(_port)
    if _host != "0.0.0.0":
        os.environ["SERVER_HOST"] = _host

    build_dir = _get_frontend_build_dir()

    print("[ldaca] Starting LDaCA Web App (backend + frontend)", flush=True)
    print(f"[ldaca] Frontend assets: {build_dir}", flush=True)
    print(f"[ldaca] Open http://localhost:{_port} in your browser", flush=True)

    import uvicorn
    from starlette.responses import FileResponse
    from starlette.staticfiles import StaticFiles

    from ldaca_web_app.main import app as fastapi_app

    index_html = build_dir / "index.html"

    # Serve static asset subdirectories (JS/CSS/images) under /assets, /tutorials, etc.
    for subdir in build_dir.iterdir():
        if subdir.is_dir():
            fastapi_app.mount(
                f"/{subdir.name}",
                StaticFiles(directory=str(subdir)),
                name=f"frontend-{subdir.name}",
            )

    # Replace the default JSON root endpoint with the frontend SPA handler.
    # Remove the existing GET / route so our catch-all takes over.
    fastapi_app.routes[:] = [
        r
        for r in fastapi_app.routes
        if not (
            hasattr(r, "path")
            and r.path == "/"
            and hasattr(r, "methods")
            and "GET" in r.methods
        )
    ]

    @fastapi_app.get("/")
    async def _serve_index():
        return FileResponse(str(index_html))

    # Catch-all for root-level static files (favicon.ico, images) and SPA fallback
    @fastapi_app.get("/{path:path}")
    async def _serve_frontend(path: str):
        file_path = build_dir / path
        if path and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(index_html))

    uvicorn.run(
        fastapi_app,
        host=_host,
        port=_port,
        log_level="info",
    )


def _run_frontend(*, port: int | None = None, host: str | None = None):
    """Serve the built frontend assets as a standalone static file server."""
    import http.server

    build_dir = _get_frontend_build_dir()
    _port = port or 3000
    _host = host or "0.0.0.0"

    print(f"[ldaca] Serving frontend from {build_dir}", flush=True)
    print(f"[ldaca] Open http://localhost:{_port} in your browser", flush=True)

    class SPAHandler(http.server.SimpleHTTPRequestHandler):
        """Serves static files with SPA fallback to index.html."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(build_dir), **kwargs)

        def do_GET(self):
            # Try the actual file first; fall back to index.html for SPA routes
            from pathlib import Path

            requested = Path(self.translate_path(self.path))
            if not requested.exists() or requested.is_dir():
                # Check if index.html exists in the directory
                if requested.is_dir() and (requested / "index.html").exists():
                    super().do_GET()
                    return
                # SPA fallback
                self.path = "/index.html"
            super().do_GET()

        def log_message(self, format, *args):
            print(f"[frontend] {args[0]}", flush=True)

    server = http.server.HTTPServer((_host, _port), SPAHandler)
    print(f"[ldaca] Frontend server listening on {_host}:{_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[ldaca] Frontend server stopped.", flush=True)
        server.server_close()


def _run_backend(*, port: int | None = None, host: str | None = None):
    """Start backend server from packaged/CLI runtime entrypoint.

    Used by:
    - `if __name__ == "__main__"` execution path in packaged and local CLI runs

    Why:
    - Coordinates startup logging, signal handling, and uvicorn app launch.

    Refactor note:
    - Function is large and handles logging/process/signal/server concerns;
        splitting into focused helpers would improve maintainability.
    """
    # Setup file logging immediately for packaged app debugging
    import os
    from datetime import datetime
    from pathlib import Path

    # Set env vars BEFORE importing settings so pydantic-settings picks them up
    if port is not None:
        os.environ["BACKEND_PORT"] = str(port)
        os.environ["LDACA_BACKEND_PORT"] = str(port)
    if host is not None:
        os.environ["SERVER_HOST"] = host

    log_file = None
    try:
        backend_runtime = os.environ.get("LDACA_BACKEND_RUNTIME")
        if backend_runtime:
            log_dir = Path(backend_runtime) / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_path = log_dir / f"cli_startup_{timestamp}.log"
            log_file = open(
                log_file_path, "w", encoding="utf-8", buffering=1
            )  # Line buffered

            # Redirect stdout/stderr to both console and file
            class TeeOutput:
                def __init__(self, file_obj, original):
                    self.file = file_obj
                    self.original = original

                def write(self, data):
                    try:
                        self.original.write(data)
                        self.original.flush()
                    except UnicodeEncodeError:
                        # Windows console may not support all Unicode characters
                        # Replace problematic characters with ASCII equivalents
                        safe_data = data.encode("ascii", "replace").decode("ascii")
                        self.original.write(safe_data)
                        self.original.flush()
                    if self.file:
                        self.file.write(data)
                        self.file.flush()

                def flush(self):
                    self.original.flush()
                    if self.file:
                        self.file.flush()

                def isatty(self):
                    # Return False for file output (no TTY colors)
                    return False

            sys.stdout = TeeOutput(log_file, sys.__stdout__)
            sys.stderr = TeeOutput(log_file, sys.__stderr__)
            print(f"[cli] Log file created: {log_file_path}", flush=True)
    except Exception as e:
        print(f"[cli] Failed to setup CLI logging: {e}", flush=True)

    print("[cli] CLI main() called", flush=True)
    import uvicorn

    print("[cli] uvicorn imported", flush=True)
    from ldaca_web_app.settings import settings

    print("[cli] settings imported", flush=True)

    # Setup cleanup handlers for child processes
    def cleanup_child_processes():
        """Terminate worker pool and child processes during shutdown.

        Used by:
        - signal handlers and `atexit` cleanup registration

        Why:
        - Prevents orphaned workers when desktop shell exits.
        """
        try:
            # Shutdown worker pool if it exists
            from ldaca_web_app.core.worker import get_worker_pool

            worker_pool = get_worker_pool()
            if worker_pool.is_running:
                print("Cleanup: Shutting down worker pool...")
                worker_pool.shutdown(wait=False)  # Don't wait during signal handler
        except Exception as e:
            print(f"Warning: Error during worker pool cleanup: {e}")

        # Kill any remaining child processes
        try:
            import os

            current_pid = os.getpid()
            # Get all children of this process
            try:
                import psutil

                parent = psutil.Process(current_pid)
                children = parent.children(recursive=True)
                if children:
                    print(f"Cleanup: Terminating {len(children)} child processes...")
                    for child in children:
                        try:
                            child.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    # Give them a moment to terminate gracefully
                    gone, alive = psutil.wait_procs(children, timeout=1)
                    # Force kill any that didn't terminate
                    for child in alive:
                        try:
                            print(f"Cleanup: Force killing process {child.pid}")
                            child.kill()
                        except psutil.NoSuchProcess:
                            pass
            except ImportError:
                # psutil not available, try basic cleanup
                print("Warning: psutil not available for comprehensive process cleanup")
        except Exception as e:
            print(f"Warning: Error during child process cleanup: {e}")

    def signal_handler(signum, frame):
        """Handle termination signals with graceful cleanup.

        Used by:
        - SIGINT/SIGTERM/SIGQUIT registrations

        Why:
        - Ensures worker cleanup runs before process exit.
        """
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        cleanup_child_processes()
        sys.exit(0)

    # Register signal handlers for graceful shutdown
    print("[cli] Registering signal handlers", flush=True)
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Kill signal
    if hasattr(signal, "SIGQUIT"):
        signal.signal(signal.SIGQUIT, signal_handler)  # Quit signal (Unix only)

    # Register cleanup to run on normal exit too
    atexit.register(cleanup_child_processes)
    print("[cli] Signal handlers registered", flush=True)

    # Ensure data root exists
    print("[cli] Getting data root", flush=True)
    data_root = settings.get_data_root()
    print(f"[cli] Creating data root: {data_root}", flush=True)
    data_root.mkdir(parents=True, exist_ok=True)
    print("[cli] Data root created", flush=True)

    print("[cli] Starting LDaCA Web App Backend", flush=True)
    print(f"[cli] Data folder: {data_root}", flush=True)

    # CLI flags override settings
    effective_host = host or settings.server_host
    effective_port = port or settings.backend_port

    print(
        f"[cli] Server: http://{effective_host}:{effective_port}",
        flush=True,
    )
    print(f"[cli] Multi-user mode: {settings.multi_user}", flush=True)
    print(flush=True)

    # Detect if running from PyInstaller bundle
    # PyInstaller sets sys.frozen = True and creates sys._MEIPASS
    is_frozen = getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")
    print(f"[cli] Is frozen: {is_frozen}", flush=True)
    print(f"[cli] Debug mode: {settings.debug}", flush=True)

    # Never use reload in frozen executables - it causes port conflicts
    # and doesn't work properly with bundled code
    use_reload = settings.debug and not is_frozen
    print(f"[cli] Use reload: {use_reload}", flush=True)

    # Import the app object directly to run uvicorn in the same process
    # Running with a string path causes uvicorn to spawn a subprocess,
    # which makes it harder to kill when Tauri terminates the parent
    print("[cli] Importing FastAPI app", flush=True)
    from ldaca_web_app.main import app as fastapi_app

    print("[cli] FastAPI app imported successfully", flush=True)

    # Run the FastAPI app in-process (not as subprocess)
    print(
        f"[cli] Starting uvicorn on {effective_host}:{effective_port}",
        flush=True,
    )
    print("[cli] Calling uvicorn.run()...", flush=True)
    try:
        uvicorn.run(
            fastapi_app,  # Pass app object directly, not string path
            host=effective_host,
            port=effective_port,
            reload=use_reload,
            log_level="info",
        )
        print("[cli] uvicorn.run() returned (server stopped)", flush=True)
    except Exception as e:
        print(f"[cli] ERROR in uvicorn.run(): {e}", flush=True)
        import traceback

        print(f"[cli] Traceback: {traceback.format_exc()}", flush=True)
        raise


if __name__ == "__main__":
    # Critical: multiprocessing guard for PyInstaller frozen executables
    # When using multiprocessing with 'spawn' method, child processes
    # re-execute the main script. We must prevent them from starting
    # additional uvicorn servers.
    import multiprocessing as mp

    print("[cli] __main__ block executed", flush=True)
    mp.freeze_support()  # Required for Windows frozen executables
    print(f"[cli] Current process: {mp.current_process().name}", flush=True)

    # Only run the server in the main process, not in worker children
    # Worker processes will have names like 'Process-1', 'Process-2', etc.
    # The main process has name 'MainProcess'
    if mp.current_process().name == "MainProcess":
        print("[cli] Running in MainProcess, starting server", flush=True)
        main()
    else:
        print(
            f"[cli] Running in child process ({mp.current_process().name}), skipping server startup",
            flush=True,
        )
    # Child processes will exit here without starting uvicorn
    # Child processes will exit here without starting uvicorn
