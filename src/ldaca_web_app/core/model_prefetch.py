"""Background prefetch of heavy ML models at startup.

Runs downloads in a daemon thread so the server starts accepting requests
immediately while models are fetched in the background. The first user who
touches the relevant feature (quotation extraction, topic modelling) gets
to skip the cold-download wait if the prefetch has already finished.
"""

from __future__ import annotations

import logging
import threading

from .worker_tasks_topic import (
    _TOPIC_EMBEDDER_REPO_ID,
    _TOPIC_EMBEDDER_REVISION,
)

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


def _prefetch_topic_embedder_mps() -> None:
    """Ensure the SentenceTransformer model weights are cached for MPS inference.

    On Apple Silicon, the worker uses SentenceTransformer + PyTorch MPS rather
    than the ONNX path. This prefetch first probes the HuggingFace cache with
    `local_files_only=True` — if the key files are already on disk, we skip the
    network entirely (matches the ONNX prefetch pattern). Only when the cache
    is incomplete do we instantiate `SentenceTransformer` to download.
    """
    try:
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import LocalEntryNotFoundError
    except Exception:
        logger.warning(
            "[prefetch] huggingface_hub not importable; MPS embedder prefetch skipped",
            exc_info=True,
        )
        return

    # Probe key files — config + tokenizer + weights (safetensors preferred,
    # pytorch_model.bin fallback). If all are cached at the pinned revision,
    # skip network entirely.
    try:
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename="config.json",
            revision=_TOPIC_EMBEDDER_REVISION,
            local_files_only=True,
        )
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename="tokenizer.json",
            revision=_TOPIC_EMBEDDER_REVISION,
            local_files_only=True,
        )
        try:
            hf_hub_download(
                repo_id=_TOPIC_EMBEDDER_REPO_ID,
                filename="model.safetensors",
                revision=_TOPIC_EMBEDDER_REVISION,
                local_files_only=True,
            )
        except LocalEntryNotFoundError:
            hf_hub_download(
                repo_id=_TOPIC_EMBEDDER_REPO_ID,
                filename="pytorch_model.bin",
                revision=_TOPIC_EMBEDDER_REVISION,
                local_files_only=True,
            )
        logger.info(
            "[prefetch] MPS embedder model already cached (revision %s)",
            _TOPIC_EMBEDDER_REVISION[:8],
        )
        return
    except (LocalEntryNotFoundError, Exception):
        pass

    try:
        from sentence_transformers import SentenceTransformer

        logger.info(
            "[prefetch] Downloading SentenceTransformer model %s @ %s for MPS...",
            _TOPIC_EMBEDDER_REPO_ID,
            _TOPIC_EMBEDDER_REVISION[:8],
        )
        SentenceTransformer(
            _TOPIC_EMBEDDER_REPO_ID,
            device="cpu",
            revision=_TOPIC_EMBEDDER_REVISION,
        )
        logger.info("[prefetch] SentenceTransformer model ready")
    except Exception:
        logger.warning("[prefetch] MPS model prefetch failed", exc_info=True)


def _prefetch_topic_embedder_onnx() -> None:
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

    # Probe primary files — if both are cached at the pinned revision we're done.
    try:
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename=onnx_filename,
            revision=_TOPIC_EMBEDDER_REVISION,
            local_files_only=True,
        )
        hf_hub_download(
            repo_id=_TOPIC_EMBEDDER_REPO_ID,
            filename="tokenizer.json",
            revision=_TOPIC_EMBEDDER_REVISION,
            local_files_only=True,
        )
        logger.info(
            "[prefetch] topic embedder ONNX files already cached (revision %s)",
            _TOPIC_EMBEDDER_REVISION[:8],
        )
        return
    except (LocalEntryNotFoundError, Exception):
        pass

    # Download: platform-appropriate model, fp32 fallback, tokenizer.
    files_to_fetch = [onnx_filename, "tokenizer.json"]
    if onnx_filename != "onnx/model.onnx":
        files_to_fetch.append("onnx/model.onnx")

    logger.info(
        "[prefetch] Downloading ONNX topic embedder %s @ %s...",
        _TOPIC_EMBEDDER_REPO_ID,
        _TOPIC_EMBEDDER_REVISION[:8],
    )
    try:
        for filename in files_to_fetch:
            try:
                hf_hub_download(
                    repo_id=_TOPIC_EMBEDDER_REPO_ID,
                    filename=filename,
                    revision=_TOPIC_EMBEDDER_REVISION,
                )
                logger.info("[prefetch] downloaded %s", filename)
            except Exception as exc:
                logger.warning("[prefetch] could not download %s: %s", filename, exc)
        logger.info("[prefetch] topic embedder download complete")
    except Exception:
        logger.warning("[prefetch] topic embedder prefetch failed", exc_info=True)


def _prefetch_topic_embedder() -> None:
    """Download the topic embedder model appropriate for this platform.

    On Apple Silicon (MPS available): downloads SentenceTransformer weights.
    On Windows/Linux/Intel Mac: downloads ONNX model + tokenizer.
    """
    from .mps_embedder import is_mps_available

    if is_mps_available():
        _prefetch_topic_embedder_mps()
    else:
        _prefetch_topic_embedder_onnx()


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
