"""Self-destruct the backend when its parent process disappears.

Tauri spawns the backend Python process with `LDACA_PARENT_PID` set to the
Tauri PID. This module starts a daemon thread that polls the parent and
exits the backend immediately if the parent goes away — covering cases
where neither Tauri's CloseRequested nor ExitRequested handler fires
(force-quit, crash, kill -9 of the desktop process, etc.).
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time

logger = logging.getLogger(__name__)

_PARENT_PID_ENV = "LDACA_PARENT_PID"
_DEFAULT_INTERVAL_SECONDS = 2.0


def _is_pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        # PROCESS_QUERY_LIMITED_INFORMATION (0x1000) is enough to call
        # GetExitCodeProcess and works without elevated privileges.
        process_query_limited_information = 0x1000
        still_active = 259
        handle = kernel32.OpenProcess(
            process_query_limited_information, False, pid
        )
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == still_active
            return False
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # The pid exists but is owned by another user; treat as alive so we
        # don't suicide on a pid recycled by an unrelated process.
        return True


def _terminate_self() -> None:
    """Kill any subprocess descendants then hard-exit this process.

    os._exit() skips Python's normal shutdown (atexit, gc, finalizers) which
    is exactly what we want — uvicorn's graceful path can hang on background
    tasks (e.g. spaCy model download), and we've already lost the parent.
    """
    try:
        import psutil

        me = psutil.Process(os.getpid())
        children = me.children(recursive=True)
        for child in children:
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception as exc:
        # Cleanup is best-effort; never block the suicide on it.
        logger.debug("Watchdog child cleanup failed: %s", exc)

    os._exit(0)


def start_parent_watchdog(
    interval_seconds: float = _DEFAULT_INTERVAL_SECONDS,
) -> None:
    """Spawn the watchdog daemon thread if a parent pid was passed in env."""

    raw = os.environ.get(_PARENT_PID_ENV)
    if not raw:
        return
    try:
        parent_pid = int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid %s value %r; parent watchdog disabled.", _PARENT_PID_ENV, raw
        )
        return
    if parent_pid <= 0:
        return

    def _run() -> None:
        logger.info(
            "Parent watchdog active (parent_pid=%d, interval=%.1fs).",
            parent_pid,
            interval_seconds,
        )
        while True:
            if not _is_pid_alive(parent_pid):
                logger.warning(
                    "Parent process %d is gone; backend self-terminating.",
                    parent_pid,
                )
                _terminate_self()
                return
            time.sleep(interval_seconds)

    threading.Thread(
        target=_run, name="ldaca-parent-watchdog", daemon=True
    ).start()
