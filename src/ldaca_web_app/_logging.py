"""Centralized logging configuration for the LDaCA backend.

Provides:
- Human-readable console logging (always active)
- Optional structured JSON file logging via ``LOG_FILE`` setting
- Per-module loggers via ``logging.getLogger(__name__)``
- ``setup_logging()`` to configure the root ``ldaca_web_app`` logger
- ``setup_file_logging()`` for TeeOutput-based log capture in packaged builds
"""

import logging
import logging.handlers
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

PACKAGE_LOGGER_NAME = "ldaca_web_app"

# Sentinel to avoid double-configuring
_logging_configured = False


class _StructuredFormatter(logging.Formatter):
    """Emit log records as structured JSON lines for machine consumption."""

    def format(self, record: logging.LogRecord) -> str:
        import json

        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str)


class _ConsoleFormatter(logging.Formatter):
    """Human-readable console format: ``TIMESTAMP LEVEL [logger] message``."""

    def __init__(self) -> None:
        super().__init__(
            fmt="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


def setup_logging(*, level: str | int | None = None) -> None:
    """Configure the ``ldaca_web_app`` logger hierarchy.

    Safe to call multiple times; only the first invocation takes effect.

    Args:
        level: Explicit log level override.  When ``None`` the level is read
            from ``settings.log_level`` (env ``LOG_LEVEL``), defaulting to
            ``INFO``.
    """
    global _logging_configured
    if _logging_configured:
        return
    _logging_configured = True

    if level is None:
        try:
            from .settings import settings

            level = settings.log_level
        except Exception:
            level = "INFO"

    numeric_level = (
        level
        if isinstance(level, int)
        else getattr(logging, str(level).upper(), logging.INFO)
    )

    root_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    root_logger.setLevel(numeric_level)

    # Avoid duplicate handlers when the function is called in odd import scenarios
    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(_ConsoleFormatter())
        root_logger.addHandler(console_handler)

    # Optional file logging when LOG_FILE is configured
    try:
        from .settings import settings

        if settings.log_file:
            log_path = Path(settings.data_root) / settings.log_file
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(_StructuredFormatter())
            root_logger.addHandler(file_handler)
    except Exception:
        pass

    # Also configure uvicorn loggers to use the same level so startup noise
    # respects the user's chosen verbosity.
    for uvicorn_logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(uvicorn_logger_name)
        uv_logger.setLevel(numeric_level)


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

    Also attaches a JSON file handler to the package logger so that structured
    records are captured alongside the tee'd stdout/stderr output.

    Returns the opened log file handle (caller should close it on shutdown),
    or ``None`` when file logging is not applicable.
    """
    logger = logging.getLogger(PACKAGE_LOGGER_NAME)

    try:
        backend_runtime = os.environ.get("LDACA_BACKEND_RUNTIME")
        if not backend_runtime:
            return None

        log_dir = Path(backend_runtime) / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = log_dir / f"{prefix}_startup_{timestamp}.log"
        log_file = open(log_file_path, "w", encoding="utf-8", buffering=1)

        original_stdout = sys.__stdout__ or sys.stdout
        original_stderr = sys.__stderr__ or sys.stderr
        sys.stdout = TeeOutput(log_file, original_stdout)  # type: ignore[assignment]
        sys.stderr = TeeOutput(log_file, original_stderr)  # type: ignore[assignment]

        # Attach a structured JSON file handler for the package logger
        json_log_path = log_dir / f"{prefix}_{timestamp}.jsonl"
        file_handler = logging.FileHandler(json_log_path, encoding="utf-8")
        file_handler.setFormatter(_StructuredFormatter())
        logger.addHandler(file_handler)

        logger.info("Log file created: %s", log_file_path)
        logger.info("Structured log file: %s", json_log_path)
        return log_file
    except Exception as e:
        logger.warning("Failed to setup file logging: %s", e)
        return None
