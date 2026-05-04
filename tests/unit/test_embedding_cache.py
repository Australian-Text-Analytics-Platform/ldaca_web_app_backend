"""Unit tests for the EmbeddingCache.

All tests use tmp_path and synthetic data — no network access required.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pytest

from ldaca_web_app.core.embedding_cache import EmbeddingCache, _hash_doc, _safe_name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cache(tmp_path: Path, model_id: str = "test/model", provider: str = "CPU") -> EmbeddingCache:
    return EmbeddingCache(cache_dir=tmp_path / "cache", model_id=model_id, provider_id=provider)


def _random_embeds(n: int, dim: int = 4, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float64).astype(np.float32)


# ---------------------------------------------------------------------------
# _safe_name
# ---------------------------------------------------------------------------


def test_safe_name_replaces_slashes_and_dots():
    assert _safe_name("sentence-transformers/all-MiniLM-L6-v2") == "sentence-transformers_all-MiniLM-L6-v2"


def test_safe_name_preserves_allowed_chars():
    assert _safe_name("abc_DEF-123") == "abc_DEF-123"


# ---------------------------------------------------------------------------
# _hash_doc
# ---------------------------------------------------------------------------


def test_hash_doc_returns_32_bytes():
    h = _hash_doc("hello")
    assert isinstance(h, bytes)
    assert len(h) == 32


def test_hash_doc_is_deterministic():
    assert _hash_doc("test") == _hash_doc("test")


def test_hash_doc_differs_for_different_texts():
    assert _hash_doc("apple") != _hash_doc("orange")


def test_hash_doc_matches_stdlib_sha256():
    text = "The quick brown fox"
    expected = hashlib.sha256(text.encode("utf-8")).digest()
    assert _hash_doc(text) == expected


# ---------------------------------------------------------------------------
# EmbeddingCache — empty / first run
# ---------------------------------------------------------------------------


def test_lookup_returns_all_missing_when_cache_absent(tmp_path):
    cache = _make_cache(tmp_path)
    docs = ["a", "b", "c"]
    embeds, missing = cache.lookup(docs)
    assert missing == [0, 1, 2]
    assert embeds.shape[0] == 3


def test_lookup_returns_zeros_placeholder_when_cache_absent(tmp_path):
    cache = _make_cache(tmp_path)
    embeds, _ = cache.lookup(["x", "y"])
    assert np.all(embeds == 0)


def test_cache_file_created_on_first_store(tmp_path):
    cache = _make_cache(tmp_path)
    assert not cache.path.exists()
    cache.store(["doc"], _random_embeds(1))
    assert cache.path.exists()


# ---------------------------------------------------------------------------
# Store + lookup round-trip
# ---------------------------------------------------------------------------


def test_round_trip_single_doc(tmp_path):
    cache = _make_cache(tmp_path)
    embeds = _random_embeds(1, dim=4)
    cache.store(["hello"], embeds)

    result, missing = cache.lookup(["hello"])
    assert missing == []
    assert result.shape == (1, 4)
    # float16 storage introduces small error; use generous tolerance
    np.testing.assert_allclose(result[0], embeds[0], atol=1e-2)


def test_round_trip_multiple_docs(tmp_path):
    cache = _make_cache(tmp_path)
    docs = ["doc_a", "doc_b", "doc_c"]
    embeds = _random_embeds(3, dim=6)
    cache.store(docs, embeds)

    result, missing = cache.lookup(docs)
    assert missing == []
    assert result.shape == (3, 6)


def test_partial_cache_hit_returns_correct_missing_indices(tmp_path):
    cache = _make_cache(tmp_path)
    cache.store(["a", "b"], _random_embeds(2))

    _, missing = cache.lookup(["a", "b", "c", "d"])
    assert missing == [2, 3]


def test_partial_cache_hit_fills_hits_correctly(tmp_path):
    cache = _make_cache(tmp_path)
    embeds = _random_embeds(2, dim=4)
    cache.store(["x", "y"], embeds)

    result, missing = cache.lookup(["x", "z", "y"])
    # index 1 ("z") is a miss; 0 ("x") and 2 ("y") are hits
    assert missing == [1]
    assert result[1].sum() == 0.0          # placeholder for miss
    np.testing.assert_allclose(result[0], embeds[0], atol=1e-2)
    np.testing.assert_allclose(result[2], embeds[1], atol=1e-2)


def test_duplicate_store_deduplicates_by_hash(tmp_path):
    cache = _make_cache(tmp_path)
    e1 = _random_embeds(1, dim=4, seed=1)
    e2 = _random_embeds(1, dim=4, seed=2)
    cache.store(["doc"], e1)
    cache.store(["doc"], e2)  # same doc, different embedding

    import polars as pl
    stored = pl.read_parquet(cache.path)
    assert len(stored) == 1   # deduplication kept only one row


def test_store_appends_new_docs(tmp_path):
    cache = _make_cache(tmp_path)
    cache.store(["a", "b"], _random_embeds(2))
    cache.store(["c"], _random_embeds(1))

    import polars as pl
    stored = pl.read_parquet(cache.path)
    assert len(stored) == 3


# ---------------------------------------------------------------------------
# EmbeddingCache.clear
# ---------------------------------------------------------------------------


def test_clear_removes_file(tmp_path):
    cache = _make_cache(tmp_path)
    cache.store(["doc"], _random_embeds(1))
    assert cache.path.exists()
    cache.clear()
    assert not cache.path.exists()


def test_clear_is_idempotent(tmp_path):
    cache = _make_cache(tmp_path)
    cache.clear()  # nothing to clear — must not raise
    cache.clear()


def test_lookup_after_clear_returns_all_missing(tmp_path):
    cache = _make_cache(tmp_path)
    cache.store(["a", "b"], _random_embeds(2))
    cache.clear()
    _, missing = cache.lookup(["a", "b"])
    assert missing == [0, 1]


# ---------------------------------------------------------------------------
# Separate files per (model, provider)
# ---------------------------------------------------------------------------


def test_different_providers_use_separate_files(tmp_path):
    c1 = EmbeddingCache(tmp_path / "cache", "m/model", "CoreML")
    c2 = EmbeddingCache(tmp_path / "cache", "m/model", "CPU")
    assert c1.path != c2.path


def test_different_models_use_separate_files(tmp_path):
    c1 = EmbeddingCache(tmp_path / "cache", "org/model-a", "CPU")
    c2 = EmbeddingCache(tmp_path / "cache", "org/model-b", "CPU")
    assert c1.path != c2.path


def test_provider_cache_isolation(tmp_path):
    """Storing via one provider must not affect lookups via another."""
    c1 = EmbeddingCache(tmp_path / "cache", "m/m", "CoreML")
    c2 = EmbeddingCache(tmp_path / "cache", "m/m", "CPU")

    c1.store(["doc"], _random_embeds(1))
    _, missing = c2.lookup(["doc"])
    assert missing == [0]   # c2 has no entry for "doc"


# ---------------------------------------------------------------------------
# _embed_with_cache integration (via worker_tasks_topic helpers)
# ---------------------------------------------------------------------------


def test_embed_with_cache_bypassed_when_no_dir(tmp_path):
    """cache_dir=None should call _encode_embeddings_in_chunks directly."""
    from ldaca_web_app.core import worker_tasks_topic

    calls: list[int] = []

    class FakeEmbedder:
        provider = "CPU"
        def encode(self, docs, show_progress_bar=False):
            calls.append(len(docs))
            return np.ones((len(docs), 4), dtype=np.float32)

    result = worker_tasks_topic._embed_with_cache(FakeEmbedder(), ["a", "b", "c"], None, None)
    assert result.shape == (3, 4)
    assert calls  # embedder was called


def test_embed_with_cache_warm_run_skips_encoder(tmp_path):
    """Second call with same docs must not invoke the embedder."""
    from ldaca_web_app.core import worker_tasks_topic

    encode_calls = []

    class FakeEmbedder:
        provider = "CPU"
        def encode(self, docs, show_progress_bar=False):
            encode_calls.append(list(docs))
            return np.random.default_rng(0).random((len(docs), 4)).astype(np.float32)

    cache_dir = str(tmp_path / "cache")
    docs = ["x", "y", "z"]

    # Cold run — populates cache
    worker_tasks_topic._embed_with_cache(FakeEmbedder(), docs, cache_dir, None)
    encode_calls.clear()

    # Warm run — should not call encoder
    result = worker_tasks_topic._embed_with_cache(FakeEmbedder(), docs, cache_dir, None)
    assert encode_calls == []
    assert result.shape == (3, 4)


def test_embed_with_cache_partial_miss_encodes_only_new_docs(tmp_path):
    """Only the uncached docs should be passed to the encoder."""
    from ldaca_web_app.core import worker_tasks_topic

    encoded_docs: list[list[str]] = []

    class FakeEmbedder:
        provider = "CPU"
        def encode(self, docs, show_progress_bar=False):
            encoded_docs.append(list(docs))
            return np.ones((len(docs), 4), dtype=np.float32) * 0.5

    cache_dir = str(tmp_path / "cache")

    # Prime cache with "a" and "b"
    worker_tasks_topic._embed_with_cache(FakeEmbedder(), ["a", "b"], cache_dir, None)
    encoded_docs.clear()

    # Request "a", "c", "b" — only "c" should be encoded
    worker_tasks_topic._embed_with_cache(FakeEmbedder(), ["a", "c", "b"], cache_dir, None)
    assert encoded_docs == [["c"]]
