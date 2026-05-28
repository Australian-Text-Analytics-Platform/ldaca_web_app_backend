"""DuckDB-backed embedding cache keyed by stable document hashes.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
    embeddings transactionally, and ignore corrupt entries without failing the worker.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

EMBEDDINGS_CACHE_FILENAME = "embeddings.duckdb"


def _safe_name(value: str) -> str:
    """Turn an arbitrary string into a safe cache key component.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
        embeddings transactionally, and ignore corrupt entries without failing the worker.
    """
    return re.sub(r"[^a-zA-Z0-9_-]", "_", value)


def _hash_doc(doc: str) -> bytes:
    """Support embedding cache persistence with a hash doc helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
        embeddings transactionally, and ignore corrupt entries without failing the worker.
    """

    return hashlib.sha256(doc.encode("utf-8")).digest()


class EmbeddingCache:
    """Read/write embedding cache backed by one DuckDB file per user cache dir.

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
        embeddings transactionally, and ignore corrupt entries without failing the worker.
    """

    def __init__(self, cache_dir: Path, model_id: str, provider_id: str) -> None:
        """Initialize EmbeddingCache state used by embedding cache persistence.

        Called by:
        - `EmbeddingCache` construction in backend services and tests because tests need the
          same observable contract that production routes and workers rely on.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """

        cache_dir.mkdir(parents=True, exist_ok=True)
        self._path = cache_dir / EMBEDDINGS_CACHE_FILENAME
        self._model_id = _safe_name(model_id)
        self._provider_id = _safe_name(provider_id)

    def _connect(self):
        """Support embedding cache persistence with a connect helper.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """

        import duckdb

        conn = duckdb.connect(str(self._path))
        self._ensure_schema(conn)
        return conn

    @staticmethod
    def _ensure_schema(conn) -> None:
        """Support embedding cache persistence with an ensure schema helper.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a stable JSON contract shared by route handlers, generated clients, and tests.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS embedding_cache (
                model_id VARCHAR NOT NULL,
                provider_id VARCHAR NOT NULL,
                content_hash VARCHAR NOT NULL,
                dimension INTEGER NOT NULL,
                dtype VARCHAR NOT NULL,
                embedding BLOB NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                last_accessed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                PRIMARY KEY (model_id, provider_id, content_hash)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS embedding_cache_lookup_idx
            ON embedding_cache (model_id, provider_id, content_hash)
            """
        )

    def lookup(self, docs: list[str]) -> tuple["np.ndarray", list[int]]:
        """Return cached embeddings and indices that still need encoding.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """
        import numpy as np

        hashes = [_hash_doc(doc).hex() for doc in docs]
        n_docs = len(docs)
        conn = self._connect()
        try:
            requested = pl.DataFrame({"content_hash": list(dict.fromkeys(hashes))})
            conn.register("requested_embedding_hashes", requested)
            rows = conn.execute(
                """
                SELECT c.content_hash, c.dimension, c.dtype, c.embedding
                FROM embedding_cache c
                JOIN requested_embedding_hashes r
                  ON c.content_hash = r.content_hash
                WHERE c.model_id = ? AND c.provider_id = ?
                """,
                [self._model_id, self._provider_id],
            ).fetchall()
            conn.unregister("requested_embedding_hashes")
        except Exception as exc:
            logger.warning(
                "[EmbeddingCache] failed to read cache %s: %s", self._path, exc
            )
            return np.zeros((n_docs, 0), dtype=np.float32), list(range(n_docs))
        finally:
            conn.close()

        if not rows:
            return np.zeros((n_docs, 0), dtype=np.float32), list(range(n_docs))

        by_hash: dict[str, np.ndarray] = {}
        dim = 0
        for content_hash, dimension, dtype, blob in rows:
            if dtype != "float16":
                continue
            dim = int(dimension)
            by_hash[str(content_hash)] = np.frombuffer(blob, dtype=np.float16).astype(
                np.float32
            )

        if dim == 0:
            return np.zeros((n_docs, 0), dtype=np.float32), list(range(n_docs))

        result = np.zeros((n_docs, dim), dtype=np.float32)
        missing: list[int] = []
        for index, content_hash in enumerate(hashes):
            embedding = by_hash.get(content_hash)
            if embedding is None:
                missing.append(index)
                continue
            result[index] = embedding
        return result, missing

    def store(self, docs: list[str], embeddings: "np.ndarray") -> None:
        """Store embeddings, replacing existing rows for the same cache key.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """
        import numpy as np

        if len(docs) == 0:
            return
        if len(docs) != int(embeddings.shape[0]):
            raise ValueError("docs and embeddings must have the same length")

        embeddings_f16 = embeddings.astype(np.float16)
        dim = int(embeddings_f16.shape[1]) if embeddings_f16.ndim == 2 else 0
        frame = pl.DataFrame(
            {
                "model_id": [self._model_id] * len(docs),
                "provider_id": [self._provider_id] * len(docs),
                "content_hash": [_hash_doc(doc).hex() for doc in docs],
                "dimension": [dim] * len(docs),
                "dtype": ["float16"] * len(docs),
                "embedding": pl.Series(
                    [row.tobytes() for row in embeddings_f16], dtype=pl.Binary
                ),
            }
        )

        conn = self._connect()
        try:
            conn.register("incoming_embeddings", frame)
            conn.execute(
                """
                INSERT OR REPLACE INTO embedding_cache
                SELECT
                    model_id,
                    provider_id,
                    content_hash,
                    dimension,
                    dtype,
                    embedding,
                    current_timestamp,
                    current_timestamp
                FROM incoming_embeddings
                """
            )
            conn.unregister("incoming_embeddings")
        finally:
            conn.close()

    def clear(self) -> None:
        """Delete the cache file if it exists.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """
        if self._path.exists():
            self._path.unlink()
            logger.info("[EmbeddingCache] cleared cache %s", self._path.name)

    @property
    def path(self) -> Path:
        """Return the persistent path used by embedding cache persistence.

        Called by:
        - `EmbeddingCache` instances owned by backend services, routes, and tests because they
          need a backend boundary that validates inputs before delegating to workspace or worker
          state.

        Flow: derive a model/provider-specific sqlite path, validate cached dimensions, store
            embeddings transactionally, and ignore corrupt entries without failing the worker.
        """

        return self._path
