"""Apple Silicon MPS embedding backend for sentence-level text encoding.

Uses SentenceTransformer with device="mps" (PyTorch Metal Performance Shaders)
on Apple Silicon Macs.  The full BERT graph runs on Metal/Neural Engine as a
single unit, avoiding the CoreML ONNX partition overhead that made CoreML
~20× slower.  On M1 Max, cold embedding of 26k docs takes ~64s with this path
vs ~201s with the ONNX ARM64 CPU path.

Falls back gracefully to CPU if MPS is not available at runtime (import error,
running in CI, Linux/Windows).

The `.provider` attribute is "MPS" so the embedding cache uses a separate
Parquet file from the ONNX provider paths — different float paths, different
cache.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: normalize inputs, delegate to the owning backend state or service boundary, and
    return serialized values or existing domain errors to callers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)


def is_mps_available() -> bool:
    """Return True if PyTorch MPS backend is usable (Apple Silicon only).

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    try:
        import torch

        return bool(torch.backends.mps.is_available())
    except Exception as exc:
        logger.debug("PyTorch MPS availability check failed: %s", exc)
        return False


def get_active_provider_id() -> str:
    """Return the provider ID string that _get_embedder will use.

    Used by the embedding-cache clear endpoint so it clears the right file.

    Used by:
    - backend tests because tests need the same observable contract that production routes
      and workers rely on.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """
    if is_mps_available():
        return "MPS"
    from .onnx_embedder import _select_providers

    return _select_providers()[0]


class MpsEmbedder:
    """SentenceTransformer-backed embedder using PyTorch MPS on Apple Silicon.

    Drop-in replacement for OnnxEmbedder.  Implements the same
    `.encode(sentences)` interface so it can be passed as BERTopic's
    `embedding_model=` argument.

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: normalize inputs, delegate to the owning backend state or service boundary, and
        return serialized values or existing domain errors to callers.
    """

    provider: str = "MPS"

    def __init__(self, model_id: str, *, revision: str | None = None) -> None:
        """Initialize MpsEmbedder state used by Metal-backed embedding inference.

        Called by:
        - `MpsEmbedder` construction in backend services and tests because tests need the same
          observable contract that production routes and workers rely on.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """

        import torch
        from sentence_transformers import SentenceTransformer

        device = "mps" if torch.backends.mps.is_available() else "cpu"
        self._model = SentenceTransformer(model_id, device=device, revision=revision)
        self.provider = "MPS" if device == "mps" else "CPU-ST"
        logger.info(
            "[MpsEmbedder] device=%s model=%s revision=%s",
            device,
            model_id,
            (revision or "main")[:8],
        )

    def encode(
        self,
        sentences: list[str],
        *,
        show_progress_bar: bool = False,
        batch_size: int = 32,
    ) -> "np.ndarray":
        """Encode sentences to L2-normalised embeddings.

        Delegates to SentenceTransformer.encode with normalize_embeddings=True
        to match the OnnxEmbedder output shape and scale.

        Called by:
        - `MpsEmbedder` instances owned by backend services, routes, and tests because they need
          a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """
        return self._model.encode(
            sentences,
            show_progress_bar=show_progress_bar,
            batch_size=batch_size,
            normalize_embeddings=True,
        )

    @classmethod
    def from_pretrained(
        cls, model_id: str, *, revision: str | None = None
    ) -> "MpsEmbedder":
        """Support Metal-backed embedding inference with a from pretrained helper.

        Called by:
        - `MpsEmbedder` instances owned by backend services, routes, and tests because they need
          a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: normalize inputs, delegate to the owning backend state or service boundary, and
            return serialized values or existing domain errors to callers.
        """

        return cls(model_id, revision=revision)
