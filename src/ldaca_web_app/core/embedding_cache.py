"""Disk-backed embedding cache keyed by SHA-256 content hash.

Embeddings are deterministic for a given (model, provider, text) tuple, so
the result of encoding a document never changes.  This cache stores those
results in a per-(model, provider) Parquet file so that re-running topic
modelling on the same corpus skips the embedding step entirely.

Storage layout (one file per (model_id, provider_id) pair):
    {cache_dir}/{sanitized_model_id}__{sanitized_provider_id}.parquet

Schema:
    hash       Binary       -- SHA-256 of the UTF-8 encoded document (32 bytes)
    embedding  List[Float16] -- 384-dimensional embedding, float16 for compactness

On lookup the file is read fully into memory.  For corpora up to a few million
documents this is fast enough (~0.5 s for 1 M rows).  If the cache grows
beyond _WARN_SIZE_BYTES a warning is logged; eviction is not yet implemented.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

_WARN_SIZE_BYTES = 500 * 1024 * 1024   # log a warning above 500 MB
_MAX_SIZE_BYTES  = 2 * 1024 * 1024 * 1024  # refuse to write above 2 GB


def _safe_name(value: str) -> str:
    """Turn an arbitrary string into a safe filename component."""
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


def _hash_doc(doc: str) -> bytes:
    return hashlib.sha256(doc.encode("utf-8")).digest()


class EmbeddingCache:
    """Read/write embedding cache backed by a Parquet file.

    Parameters
    ----------
    cache_dir:
        Directory where the Parquet file will be stored.  Created on demand.
    model_id:
        HuggingFace model repo ID (used in the filename).
    provider_id:
        ONNX Runtime execution provider name (used in the filename).
        Different providers can produce slightly different floating-point
        results, so they get separate cache files.
    """

    def __init__(self, cache_dir: Path, model_id: str, provider_id: str) -> None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{_safe_name(model_id)}__{_safe_name(provider_id)}.parquet"
        self._path = cache_dir / filename

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(
        self, docs: list[str]
    ) -> tuple["np.ndarray", list[int]]:
        """Return (embeddings, missing_indices).

        ``embeddings`` is a float32 ndarray of shape ``(len(docs), D)`` where
        cached rows are filled in and rows for missing docs are zero.
        ``missing_indices`` lists the positions in ``docs`` that were not found
        in the cache and must still be encoded.
        """
        import numpy as np

        n = len(docs)
        hashes = [_hash_doc(doc) for doc in docs]

        if not self._path.exists():
            return np.zeros((n, 0), dtype=np.float32), list(range(n))

        import polars as pl

        try:
            stored = pl.read_parquet(self._path)
        except Exception as exc:
            logger.warning("[EmbeddingCache] failed to read cache %s: %s", self._path, exc)
            return np.zeros((n, 0), dtype=np.float32), list(range(n))

        if stored.is_empty():
            return np.zeros((n, 0), dtype=np.float32), list(range(n))

        # Build a mapping: hash_bytes → row index in stored
        stored_hashes: list[bytes] = stored["hash"].to_list()
        hash_to_row: dict[bytes, int] = {h: i for i, h in enumerate(stored_hashes)}

        # Determine embedding dimensionality from first stored row
        first_emb = stored["embedding"][0].to_list()
        dim = len(first_emb)

        result = np.zeros((n, dim), dtype=np.float32)
        missing: list[int] = []

        stored_embs = stored["embedding"].to_list()
        for i, h in enumerate(hashes):
            row = hash_to_row.get(h)
            if row is not None:
                result[i] = np.array(stored_embs[row], dtype=np.float32)
            else:
                missing.append(i)

        return result, missing

    def store(self, docs: list[str], embeddings: "np.ndarray") -> None:
        """Append (hash, embedding) rows to the cache, deduplicating by hash."""
        import numpy as np
        import polars as pl

        if len(docs) == 0:
            return

        # Check disk budget before writing
        if self._path.exists():
            size = self._path.stat().st_size
            if size >= _MAX_SIZE_BYTES:
                logger.warning(
                    "[EmbeddingCache] cache %s exceeds %d GB limit; skipping write",
                    self._path.name,
                    _MAX_SIZE_BYTES // (1024 ** 3),
                )
                return
            if size >= _WARN_SIZE_BYTES:
                logger.warning(
                    "[EmbeddingCache] cache %s is %.0f MB; consider clearing it",
                    self._path.name,
                    size / (1024 ** 2),
                )

        hashes = [_hash_doc(doc) for doc in docs]
        embs_f16 = embeddings.astype(np.float16)

        new_frame = pl.DataFrame(
            {
                "hash": pl.Series(hashes, dtype=pl.Binary),
                "embedding": embs_f16.tolist(),
            }
        )

        if self._path.exists():
            try:
                existing = pl.read_parquet(self._path)
                combined = pl.concat([existing, new_frame])
            except Exception as exc:
                logger.warning(
                    "[EmbeddingCache] failed to read cache for merge: %s; overwriting",
                    exc,
                )
                combined = new_frame
        else:
            combined = new_frame

        # Keep the last occurrence per hash (newest wins)
        combined = combined.unique(subset=["hash"], keep="last", maintain_order=False)
        combined.write_parquet(self._path)
        logger.debug(
            "[EmbeddingCache] stored %d rows; cache now %d rows",
            len(docs),
            len(combined),
        )

    def clear(self) -> None:
        """Delete the cache file if it exists."""
        if self._path.exists():
            self._path.unlink()
            logger.info("[EmbeddingCache] cleared cache %s", self._path.name)

    @property
    def path(self) -> Path:
        return self._path
