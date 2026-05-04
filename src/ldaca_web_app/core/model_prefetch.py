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

# ONNX model + tokenizer used by topic modelling (BERTopic embeddings). Must
# stay in sync with _TOPIC_EMBEDDER_REPO_ID in worker_tasks_topic.py.
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
    """Download ONNX model + tokenizer for topic modelling if not cached.

    Downloads only the files needed by OnnxEmbedder (quantized model +
    tokenizer), skipping the full safetensors weights that were previously
    fetched via snapshot_download.  hf_hub_download is idempotent — cached
    files are returned instantly without hitting the network.
    """
    try:
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import LocalEntryNotFoundError
    except Exception:
        logger.warning(
            "[prefetch] huggingface_hub not importable; topic embedder prefetch skipped",
            exc_info=True,
        )
        return

    from .onnx_embedder import _select_onnx_filename, _select_providers

    providers = _select_providers()
    onnx_filename = _select_onnx_filename(providers)

    # Probe primary files — if both are cached we're done.
    try:
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename=onnx_filename,
            local_files_only=True,
        )
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename="tokenizer.json",
            local_files_only=True,
        )
        logger.info("[prefetch] topic embedder ONNX files already cached")
        return
    except (LocalEntryNotFoundError, Exception):
        pass

    # Download: platform-appropriate model, fp32 fallback, tokenizer.
    files_to_fetch = [onnx_filename, "tokenizer.json"]
    if onnx_filename != "onnx/model.onnx":
        files_to_fetch.append("onnx/model.onnx")

    logger.info(
        "[prefetch] Downloading ONNX topic embedder %s...",
        _TOPIC_EMBEDDER_REPO_ID,
    )
    try:
        for filename in files_to_fetch:
            try:
                hf_hub_download(repo_id=_TOPIC_EMBEDDER_REPO_ID, filename=filename)
                logger.info("[prefetch] downloaded %s", filename)
            except Exception as exc:
                logger.warning("[prefetch] could not download %s: %s", filename, exc)
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
