"""Background prefetch of heavy ML models at startup.

Runs downloads in a daemon thread so the server starts accepting requests
immediately while models are fetched in the background.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)


def _prefetch_spacy_model() -> None:
    """Download the spaCy model used by quotation extraction if not cached."""
    try:
        from .quotation_extractor import (
            _download_spacy_model_to_cache,
            _get_cached_spacy_model_dir,
        )

        cached_dir = _get_cached_spacy_model_dir()
        if (cached_dir / "config.cfg").exists():
            logger.info("[prefetch] spaCy model already cached at %s", cached_dir)
            return

        logger.info("[prefetch] Downloading spaCy model in background...")
        _download_spacy_model_to_cache()
        logger.info("[prefetch] spaCy model download complete")
    except Exception:
        logger.warning("[prefetch] spaCy model prefetch failed", exc_info=True)


def start_model_prefetch() -> None:
    """Kick off background model downloads in a daemon thread."""
    thread = threading.Thread(
        target=_prefetch_spacy_model,
        name="model-prefetch",
        daemon=True,
    )
    thread.start()
