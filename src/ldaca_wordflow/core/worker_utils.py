"""Worker task decorator shared by worker.py and worker task modules.

Extracted from worker.py to avoid circular imports when task modules need the
decorator but worker.py already imports from them.
"""

from __future__ import annotations

import functools
import logging
import os
from typing import Any, Callable, cast

logger = logging.getLogger(__name__)


def _configure_worker_environment() -> None:
    """Initialize worker process runtime environment."""
    import importlib
    import importlib.util

    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

    tbb_available = False
    try:
        if importlib.util.find_spec("tbb"):
            importlib.import_module("tbb")
            tbb_available = True
        elif importlib.util.find_spec("tbb4py"):
            importlib.import_module("tbb4py")
            tbb_available = True
    except Exception:
        tbb_available = False

    if tbb_available:
        os.environ.setdefault("NUMBA_THREADING_LAYER_PRIORITY", "tbb workqueue omp")
        os.environ.setdefault("NUMBA_THREADING_LAYER", "tbb")
    else:
        os.environ.setdefault("NUMBA_THREADING_LAYER", "workqueue")
        os.environ.setdefault("NUMBA_THREADING_LAYER_PRIORITY", "workqueue omp tbb")
        os.environ.setdefault("NUMBA_NUM_THREADS", "1")


configure_worker_environment = _configure_worker_environment


def worker_task(func: Callable) -> Callable:
    """Decorator for worker task functions.

    - Calls configure_worker_environment() before the task function
    - Wraps in try/except with consistent logging
    - Calls progress_callback(-1, msg) on error, then re-raises
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _configure_worker_environment()
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error("[Worker] %s failed: %s", getattr(func, "__name__", "unknown"), e)
            progress_callback = kwargs.get("progress_callback")
            if progress_callback is not None:
                try:
                    progress_callback(-1, f"Failed: {str(e)}")
                except Exception as cb_exc:
                    logger.debug("progress_callback during error handling failed: %s", cb_exc)
            raise

    wrapper.__wrapped__ = func
    return wrapper
