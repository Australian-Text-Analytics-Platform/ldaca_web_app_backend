"""Unit tests for the DuckDB-backed embedding cache."""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb
import numpy as np
from ldaca_wordflow.core.embedding_cache import EmbeddingCache, _hash_doc, _safe_name


def _make_cache(
    tmp_path: Path,
    model_id: str = "test/model",
    provider: str = "CPU",
) -> EmbeddingCache:
    return EmbeddingCache(
        cache_dir=tmp_path / "cache",
        model_id=model_id,
        provider_id=provider,
    )


def _random_embeds(n: int, dim: int = 4, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float64).astype(np.float32)


def test_safe_name_replaces_slashes_and_dots() -> None:
    assert (
        _safe_name("sentence-transformers/all-MiniLM-L6-v2")
        == "sentence-transformers_all-MiniLM-L6-v2"
    )


def test_hash_doc_matches_stdlib_sha256() -> None:
    text = "The quick brown fox"
    expected = hashlib.sha256(text.encode("utf-8")).digest()
    assert _hash_doc(text) == expected


def test_lookup_creates_missing_duckdb_file_with_schema(tmp_path: Path) -> None:
    cache = _make_cache(tmp_path)
    assert not cache.path.exists()

    embeds, missing = cache.lookup(["a", "b"])

    assert cache.path == tmp_path / "cache" / "embeddings.duckdb"
    assert cache.path.exists()
    assert embeds.shape == (2, 0)
    assert missing == [0, 1]
    with duckdb.connect(str(cache.path), read_only=True) as conn:
        columns = conn.execute("DESCRIBE embedding_cache").fetchall()
    assert [row[0] for row in columns][:5] == [
        "model_id",
        "provider_id",
        "content_hash",
        "dimension",
        "dtype",
    ]


def test_round_trip_multiple_docs(tmp_path: Path) -> None:
    cache = _make_cache(tmp_path)
    docs = ["doc_a", "doc_b", "doc_c"]
    embeds = _random_embeds(3, dim=6)

    cache.store(docs, embeds)
    result, missing = cache.lookup(docs)

    assert missing == []
    assert result.shape == (3, 6)
    np.testing.assert_allclose(result, embeds, atol=1e-2)


def test_partial_cache_hit_returns_correct_missing_indices(tmp_path: Path) -> None:
    cache = _make_cache(tmp_path)
    cache.store(["a", "b"], _random_embeds(2))

    result, missing = cache.lookup(["a", "b", "c", "d"])

    assert missing == [2, 3]
    assert result.shape == (4, 4)


def test_model_and_provider_are_part_of_embedding_key(tmp_path: Path) -> None:
    c1 = EmbeddingCache(tmp_path / "cache", "m/model", "CoreML")
    c2 = EmbeddingCache(tmp_path / "cache", "m/model", "CPU")

    c1.store(["doc"], _random_embeds(1))
    _, missing = c2.lookup(["doc"])

    assert c1.path == c2.path
    assert missing == [0]


def test_clear_removes_duckdb_file(tmp_path: Path) -> None:
    cache = _make_cache(tmp_path)
    cache.store(["doc"], _random_embeds(1))
    assert cache.path.exists()

    cache.clear()

    assert not cache.path.exists()
