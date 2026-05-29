"""``_select_embedder`` routes to a multilingual model when the corpus language isn't English.

Locks in the routing decision without loading the actual model so the test is fast.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

from ldaca_wordflow.core import worker_tasks_topic_embedding as topic_embedding
from ldaca_wordflow.core.worker_tasks_topic_embedding import (
    _TOPIC_EMBEDDERS_BY_LANGUAGE,
    _embedder_cache_label,
    _embedder_provider_id,
    _get_embedder,
    _select_embedder,
)


def test_english_keeps_pinned_minilm_l6() -> None:
    repo_id, revision = _select_embedder("en")
    assert repo_id == "sentence-transformers/all-MiniLM-L6-v2"
    # The English embedder stays pinned so existing flows are byte-identical.
    assert revision is not None and len(revision) >= 16


def test_chinese_routes_to_multilingual_minilm_l12() -> None:
    repo_id, _revision = _select_embedder("zh")
    assert repo_id == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def test_japanese_routes_to_multilingual_minilm_l12() -> None:
    repo_id, _revision = _select_embedder("ja")
    assert repo_id == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def test_unknown_language_routes_to_multilingual() -> None:
    """Anything other than ``"en"`` should hit the multilingual fallback —
    that's the safe default for an unseen language code."""
    repo_id, _revision = _select_embedder("xx")
    assert repo_id == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def test_none_language_defaults_to_english_for_backward_compat() -> None:
    repo_id, _revision = _select_embedder(None)
    assert repo_id == "sentence-transformers/all-MiniLM-L6-v2"


def test_language_normalisation_handles_case_and_whitespace() -> None:
    repo_id_a, _ = _select_embedder(" EN ")
    repo_id_b, _ = _select_embedder("Zh")
    assert repo_id_a == "sentence-transformers/all-MiniLM-L6-v2"
    assert repo_id_b == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def test_cache_label_format_includes_revision_prefix() -> None:
    en_label = _embedder_cache_label(*_TOPIC_EMBEDDERS_BY_LANGUAGE["en"])
    multi_label = _embedder_cache_label(*_TOPIC_EMBEDDERS_BY_LANGUAGE["multi"])
    english_revision = _TOPIC_EMBEDDERS_BY_LANGUAGE["en"][1]
    assert english_revision is not None
    # English embedder pinned → revision prefix appears.
    assert en_label.endswith("@" + english_revision[:8])
    # Multilingual embedder unpinned → "latest" sentinel keeps cache files
    # distinct from the English ones until a revision pin lands.
    assert multi_label.endswith("@latest")
    # Critical contract: the two cache labels must be distinct so a shared
    # cache dir can hold embeddings for both models without collision.
    assert en_label != multi_label


def test_get_embedder_uses_native_sentence_transformer(monkeypatch) -> None:
    calls: list[tuple[str, str | None]] = []

    class FakeSentenceTransformer:
        device = "test-device"

        def __init__(self, model_id: str, *, revision: str | None = None) -> None:
            calls.append((model_id, revision))

    original_cache = dict(topic_embedding._EMBEDDER_CACHE)
    topic_embedding._EMBEDDER_CACHE.clear()
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        SimpleNamespace(SentenceTransformer=FakeSentenceTransformer),
    )

    try:
        first = _get_embedder("sentence-transformers/test-model", revision="abc123")
        second = _get_embedder("sentence-transformers/test-model", revision="abc123")
    finally:
        topic_embedding._EMBEDDER_CACHE.clear()
        topic_embedding._EMBEDDER_CACHE.update(original_cache)

    assert first is second
    assert calls == [("sentence-transformers/test-model", "abc123")]
    assert _embedder_provider_id(first) == "sentence-transformers:test-device"
