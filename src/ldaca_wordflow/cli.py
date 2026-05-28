"""Command-line interface for LDaCA Web App.

Usage:
    uvx ldaca-wordflow                  # Launch full app (backend + frontend)
    uvx ldaca-wordflow --backend        # Launch backend only
    uvx ldaca-wordflow --frontend       # Launch frontend only (requires built assets)
    uvx ldaca-wordflow --port 9000      # Custom port

Used by:
- Backend package imports, application startup, and backend tests because tests need the
  same observable contract that production routes and workers rely on.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

import atexit
import logging
import signal
import sys

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None):
    """Support command-line server startup with a parse args helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    import argparse

    parser = argparse.ArgumentParser(
        prog="ldaca-wordflow",
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
            "Enable multi-user mode with OAuth/OIDC login. "
            "Requires GOOGLE_CLIENT_ID (Google OAuth) or CILOGON_CLIENT_ID (CILogon OIDC) "
            "to be set in the environment."
        ),
    )
    return parser.parse_args(argv)


def _setup_signal_handlers() -> None:
    """Register signal handlers and atexit hooks for graceful CLI shutdown.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    def _cleanup_child_processes() -> None:
        """Terminate child processes during shutdown (Tauri/packaged desktop).

        Called by:
        - The `_setup_signal_handlers` local workflow in this module because the local shared
          backend behavior flow needs this step kept close to the code that consumes it.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """
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

    def _signal_handler(signum, _frame):
        """Support command-line server startup with a signal handler helper.

        Called by:
        - The `_setup_signal_handlers` local workflow in this module because the local shared
          backend behavior flow needs this step kept close to the code that consumes it.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """

        logger.info("Received signal %s, shutting down gracefully...", signum)
        _cleanup_child_processes()
        sys.exit(0)

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    if hasattr(signal, "SIGQUIT"):
        signal.signal(signal.SIGQUIT, _signal_handler)

    atexit.register(_cleanup_child_processes)


def _open_browser_after_delay(port: int, delay: float = 1.5) -> None:
    """Open the user's browser after a short delay (non-blocking).

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    import threading
    import webbrowser

    def _open():
        """Support command-line server startup with an open helper.

        Called by:
        - The `_open_browser_after_delay` local workflow in this module because the local shared
          backend behavior flow needs this step kept close to the code that consumes it.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """

        import time

        time.sleep(delay)
        webbrowser.open(f"http://localhost:{port}")

    threading.Thread(target=_open, daemon=True).start()


def main(argv: list[str] | None = None):
    """CLI entry point dispatching to backend, frontend, or both.

    Used by:
    - FastAPI application startup, backend API routes, backend package imports, backend
      tests, core workspace and worker services, local helpers in this module because they
      need a backend boundary that validates inputs before delegating to workspace or worker
      state.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    args = _parse_args(argv)

    # Set env vars BEFORE any imports that trigger settings initialization
    if args.multi_user:
        import os

        os.environ["MULTI_USER"] = "true"
        has_google = bool(os.environ.get("GOOGLE_CLIENT_ID", "").strip())
        has_cilogon = bool(os.environ.get("CILOGON_CLIENT_ID", "").strip())
        if not has_google and not has_cilogon:
            logger.error(
                "Multi-user mode requires an OAuth/OIDC provider. "
                "Set GOOGLE_CLIENT_ID for Google OAuth or "
                "CILOGON_CLIENT_ID + CILOGON_CLIENT_SECRET for CILogon OIDC."
            )
            sys.exit(2)

        # The package __init__ eagerly imports .main, which constructs the
        # Settings singleton before main() runs. Refresh it in place so every
        # `from .settings import settings` binding sees MULTI_USER=true.
        from .settings import reload_settings

        reload_settings()

    from ._logging import setup_file_logging, setup_logging
    from .core.parent_watchdog import start_parent_watchdog

    setup_logging()
    setup_file_logging("cli")
    _setup_signal_handlers()
    # Self-destruct if the Tauri parent dies (force-quit, crash, kill -9).
    # No-op in non-Tauri runs because LDACA_PARENT_PID is unset.
    start_parent_watchdog()

    # Mutually exclusive argparse group: at most one of these is True.
    # No flag  -> both   (default: full app)
    # --backend -> backend only
    # --frontend -> frontend only
    use_backend = not args.frontend
    use_frontend = not args.backend

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
    # Multiprocessing guard for PyInstaller frozen executables: on Windows the
    # module is re-imported in worker processes; freeze_support() returns only
    # in the main process, so the call below is a no-op for children.
    import multiprocessing as mp

    mp.freeze_support()
    if mp.current_process().name == "MainProcess":
        main()
