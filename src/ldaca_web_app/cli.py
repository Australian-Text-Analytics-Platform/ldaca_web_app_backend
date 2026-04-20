"""
Command-line interface for LDaCA Web App.

Usage:
    uvx ldaca-web-app                  # Launch full app (backend + frontend)
    uvx ldaca-web-app --backend        # Launch backend only
    uvx ldaca-web-app --frontend       # Launch frontend only (requires built assets)
    uvx ldaca-web-app --port 9000      # Custom port
"""

import atexit
import logging
import signal
import sys

logger = logging.getLogger(__name__)


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
    parser.add_argument(
        "--multi-user",
        action="store_true",
        help=(
            "Enable multi-user mode with Google OAuth login. "
            "Requires the GOOGLE_CLIENT_ID environment variable to be set."
        ),
    )
    return parser.parse_args(argv)


def _setup_signal_handlers() -> None:
    """Register signal handlers and atexit hooks for graceful CLI shutdown."""

    def _cleanup_child_processes() -> None:
        """Terminate child processes during shutdown (Tauri/packaged desktop)."""
        try:
            import os

            import psutil

            parent = psutil.Process(os.getpid())
            children = parent.children(recursive=True)
            if children:
                logger.info("Cleanup: Terminating %d child processes...", len(children))
                for child in children:
                    try:
                        child.terminate()
                    except psutil.NoSuchProcess:
                        pass
                gone, alive = psutil.wait_procs(children, timeout=1)
                for child in alive:
                    try:
                        logger.warning("Cleanup: Force killing process %s", child.pid)
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
        except ImportError:
            logger.warning("psutil not available for comprehensive process cleanup")
        except Exception as e:
            logger.warning("Error during child process cleanup: %s", e)

    def _signal_handler(signum, frame):
        logger.info("Received signal %s, shutting down gracefully...", signum)
        _cleanup_child_processes()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    if hasattr(signal, "SIGQUIT"):
        signal.signal(signal.SIGQUIT, _signal_handler)

    atexit.register(_cleanup_child_processes)


def _open_browser_after_delay(port: int, delay: float = 1.5) -> None:
    """Open the user's browser after a short delay (non-blocking)."""
    import threading
    import webbrowser

    def _open():
        import time

        time.sleep(delay)
        webbrowser.open(f"http://localhost:{port}")

    threading.Thread(target=_open, daemon=True).start()


def main(argv: list[str] | None = None):
    """CLI entry point dispatching to backend, frontend, or both."""
    args = _parse_args(argv)

    # Set env vars BEFORE any imports that trigger settings initialization
    if args.multi_user:
        import os

        os.environ["MULTI_USER"] = "true"
        if not os.environ.get("GOOGLE_CLIENT_ID", "").strip():
            logger.error(
                "Multi-user mode requires the GOOGLE_CLIENT_ID environment variable. "
                "Start with: GOOGLE_CLIENT_ID=<your-client-id> ldaca-web-app --multi-user"
            )
            sys.exit(2)

        # The package __init__ eagerly imports .main, which constructs the
        # Settings singleton before main() runs. Refresh it in place so every
        # `from .settings import settings` binding sees MULTI_USER=true.
        from .settings import reload_settings

        reload_settings()

    from ._logging import setup_file_logging, setup_logging

    setup_logging()
    setup_file_logging("cli")
    _setup_signal_handlers()

    use_backend = not args.frontend if not args.backend else True
    use_frontend = not args.backend if not args.frontend else False

    # Resolve effective port for the browser-open helper
    if args.port:
        effective_port = args.port
    elif use_backend:
        effective_port = 8001
    else:
        effective_port = 3000

    # Only auto-open the browser when the frontend is being served. In
    # backend-only mode (e.g. launched by the Tauri desktop shell) the UI is
    # provided by the native window, so popping a browser window is noisy.
    if use_frontend:
        _open_browser_after_delay(effective_port)

    from .main import start_server

    start_server(
        backend=use_backend,
        frontend=use_frontend,
        port=args.port,
        host=args.host,
    )


if __name__ == "__main__":
    # Multiprocessing guard for PyInstaller frozen executables
    import multiprocessing as mp

    mp.freeze_support()
    main()

    # Only run the server in the main process, not in worker children
    # Worker processes will have names like 'Process-1', 'Process-2', etc.
    # The main process has name 'MainProcess'
    if mp.current_process().name == "MainProcess":
        logger.info("Running in MainProcess, starting server")
        main()
    else:
        logger.debug(
            "Running in child process (%s), skipping server startup",
            mp.current_process().name,
        )
    # Child processes will exit here without starting uvicorn
    # Child processes will exit here without starting uvicorn
