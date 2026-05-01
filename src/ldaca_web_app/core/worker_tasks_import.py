"""LDaCA import worker task implementation."""

from __future__ import annotations

import logging
import os
from contextlib import chdir
from typing import Any, Callable, Dict, Optional

from ldaca_web_app.settings import settings

logger = logging.getLogger(__name__)


def _sanitize_name(name: str) -> str:
    """Sanitize a corpus name for use as a folder/file name."""
    import re

    sanitized = re.sub(r"[^\w.~-]", "_", name)
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized.strip("_") or "ldaca_import"


def run_ldaca_import_task(
    configure_worker_environment,
    user_id: str,
    workspace_id: str,
    url: str,
    filename: Optional[str] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """Execute LDaCA dataset import in a worker process.

    Creates a per-corpus folder under ``LDaCA/`` containing:
    - ``<corpus_name>.parquet`` — the tabulated text data
    - ``README.md`` — corpus metadata from ``get_corpus_info()``
    """
    configure_worker_environment()

    try:
        from ldacatabulator.tabulator import LDaCATabulator
    except ImportError:
        raise RuntimeError(
            "ldaca-loader is not installed. "
            "Install with: pip install 'ldaca-web-app[ldaca]'"
        )

    from ldaca_web_app.core.utils import get_user_data_folder

    logger.info(
        "[Worker %d] Starting LDaCA import task for user %s", os.getpid(), user_id
    )

    try:
        if progress_callback:
            progress_callback(0.1, "Connecting to LDaCA...")

        cache_dir = settings.get_data_root() / "ldaca_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        with chdir(cache_dir):
            if progress_callback:
                progress_callback(0.3, "Downloading and extracting...")

            ldac_tb = LDaCATabulator(url)
            corpus_name = ldac_tb.get_name()
            sanitized = _sanitize_name(corpus_name)

            # README metadata is best-effort; failure here must not abort the import.
            try:
                corpus_info_md = ldac_tb.get_corpus_info()
            except Exception:
                corpus_info_md = None

            if progress_callback:
                progress_callback(0.6, "Converting to DataFrame...")

            df = ldac_tb.get_text()

        if progress_callback:
            progress_callback(0.8, "Saving to user data...")

        user_data_folder = get_user_data_folder(user_id)
        ldaca_folder = user_data_folder / "LDaCA"

        # Create a per-corpus subfolder, suffixing if the name is taken.
        corpus_folder = ldaca_folder / sanitized
        counter = 1
        base_folder = corpus_folder
        while corpus_folder.exists():
            corpus_folder = base_folder.parent / f"{base_folder.name}_{counter}"
            counter += 1
        corpus_folder.mkdir(parents=True, exist_ok=True)

        file_path = corpus_folder / f"{sanitized}.parquet"
        df.to_parquet(str(file_path))

        if corpus_info_md:
            (corpus_folder / "README.md").write_text(corpus_info_md, encoding="utf-8")

        if progress_callback:
            progress_callback(1.0, "Import completed successfully")

        logger.info("[Worker %d] LDaCA import completed: %s", os.getpid(), file_path)

        return {
            "success": True,
            "filename": file_path.name,
            "path": str(file_path),
            "size": file_path.stat().st_size,
            "message": f"Successfully imported {corpus_name}",
        }

    except Exception as e:
        logger.error("[Worker %d] LDaCA import failed: %s", os.getpid(), e)
        if progress_callback:
            progress_callback(-1, f"Failed: {str(e)}")
        raise
