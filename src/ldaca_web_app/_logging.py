"""Shared file-logging helpers for packaged-app runtime debugging."""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import IO


class TeeOutput:
    """Duplicate writes to both a file and the original stream.

    Handles Windows console UnicodeEncodeError by replacing
    problematic characters with ASCII equivalents.
    """

    def __init__(self, file_obj: IO[str], original: IO[str]):
        self.file = file_obj
        self.original = original

    def write(self, data: str) -> int:
        try:
            self.original.write(data)
            self.original.flush()
        except UnicodeEncodeError:
            safe_data = data.encode("ascii", "replace").decode("ascii")
            self.original.write(safe_data)
            self.original.flush()
        if self.file:
            self.file.write(data)
            self.file.flush()
        return len(data)

    def flush(self) -> None:
        self.original.flush()
        if self.file:
            self.file.flush()

    def isatty(self) -> bool:
        return False


def setup_file_logging(prefix: str) -> IO[str] | None:
    """Redirect stdout/stderr to a timestamped log file when running inside a
    packaged backend runtime (``LDACA_BACKEND_RUNTIME`` is set).

    Returns the opened log file handle (caller should close it on shutdown),
    or ``None`` when file logging is not applicable.
    """
    try:
        backend_runtime = os.environ.get("LDACA_BACKEND_RUNTIME")
        if not backend_runtime:
            return None

        log_dir = Path(backend_runtime) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = log_dir / f"{prefix}_startup_{timestamp}.log"
        log_file = open(log_file_path, "w", encoding="utf-8", buffering=1)

        sys.stdout = TeeOutput(log_file, sys.__stdout__)  # type: ignore[assignment]
        sys.stderr = TeeOutput(log_file, sys.__stderr__)  # type: ignore[assignment]
        print(f"[{prefix}] Log file created: {log_file_path}", flush=True)
        return log_file
    except Exception as e:
        print(f"[{prefix}] Failed to setup file logging: {e}", flush=True)
        return None
