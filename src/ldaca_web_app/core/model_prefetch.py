"""Background prefetch of heavy ML models at startup.

Runs downloads in a daemon thread so the server starts accepting requests
immediately while models are fetched in the background. The first user who
touches the relevant feature (quotation extraction, topic modelling) gets
to skip the cold-download wait if the prefetch has already finished.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

# Sentence-transformer used by topic modelling (BERTopic embeddings). Must
# stay in sync with `embedding_model_name` in worker_tasks_topic.py.
_TOPIC_EMBEDDER_REPO_ID = "sentence-transformers/all-MiniLM-L6-v2"


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


def _prefetch_topic_embedder() -> None:
    """Download the sentence-transformer used by topic modelling if not cached.

    `snapshot_download` is idempotent: when the repo is already in the
    Hugging Face cache it just returns the local path without contacting
    the network. We probe with `local_files_only=True` first so we can log
    the cached/downloading distinction the same way the spaCy path does.
    Files only — we deliberately don't instantiate `SentenceTransformer`,
    since the worker process loads it itself and we don't want to keep
    ~80MB of weights in the main process's memory.
    """
    try:
        from huggingface_hub import snapshot_download
        from huggingface_hub.errors import LocalEntryNotFoundError
    except Exception:
        logger.warning(
            "[prefetch] huggingface_hub not importable; topic embedder prefetch skipped",
            exc_info=True,
        )
        return

    try:
        try:
            cached_path = snapshot_download(
                repo_id=_TOPIC_EMBEDDER_REPO_ID, local_files_only=True
            )
            logger.info(
                "[prefetch] topic embedder already cached at %s", cached_path
            )
            return
        except LocalEntryNotFoundError:
            pass

        logger.info(
            "[prefetch] Downloading topic embedder %s in background...",
            _TOPIC_EMBEDDER_REPO_ID,
        )
        snapshot_download(repo_id=_TOPIC_EMBEDDER_REPO_ID)
        logger.info("[prefetch] topic embedder download complete")
    except Exception:
        logger.warning("[prefetch] topic embedder prefetch failed", exc_info=True)


def _run_all_prefetches() -> None:
    """Run each prefetch sequentially in the daemon thread."""
    _prefetch_spacy_model()
    _prefetch_topic_embedder()


def start_model_prefetch() -> None:
    """Kick off background model downloads in a daemon thread."""
    thread = threading.Thread(
        target=_run_all_prefetches,
        name="model-prefetch",
        daemon=True,
    )
    thread.start()
