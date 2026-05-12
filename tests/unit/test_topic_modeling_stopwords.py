"""Phase 3.5: label-stage CountVectorizer stop_words is language-routed.

BERTopic's clustering stage works on document embeddings (no stopwords
involved), but the per-topic label stage uses a CountVectorizer to pick
"topic-distinguishing" terms via c-TF-IDF. ``OnlineCountVectorizer`` was
hardwired to ``stop_words="english"``, which on a Chinese corpus is wrong
in two ways:

1. It silently keeps every Chinese function word (的 / 是 / 了 / 在 / 我 /
   你) as a candidate, so they dominate every topic label.
2. It looks like stopword filtering is happening when it isn't.

The new behaviour: ``"english"`` only when the resolved language is
English; ``None`` (no filter) otherwise. Future work can wire in
per-language stopword lists at no API change.
"""

from __future__ import annotations

import pytest

from ldaca_web_app.core.worker_tasks_topic import _build_online_pipeline

bertopic = pytest.importorskip("bertopic")


def _make_dummy_embedder():
    class _Embedder:
        def encode(self, *_args, **_kwargs):
            raise AssertionError(
                "encode() must not run at pipeline-build time — only the "
                "vectorizer routing matters for this test"
            )

    return _Embedder()


def _vectorizer(topic_model):
    """Pull the actual vectorizer instance off a BERTopic model."""
    return getattr(topic_model, "vectorizer_model", None) or getattr(
        topic_model, "_vectorizer_model", None
    )


def test_english_keeps_sklearn_english_stopwords() -> None:
    model, _k = _build_online_pipeline(
        n_docs=100,
        n_clusters=5,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="en",
    )
    vec = _vectorizer(model)
    assert vec is not None, (
        "online pipeline should attach an OnlineCountVectorizer"
    )
    # sklearn's English stopword list path: vec.stop_words is the string
    # "english" until fit() expands it.
    assert vec.stop_words == "english"


def test_chinese_disables_english_stopwords() -> None:
    model, _k = _build_online_pipeline(
        n_docs=100,
        n_clusters=5,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="zh",
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words is None


def test_japanese_disables_english_stopwords() -> None:
    model, _k = _build_online_pipeline(
        n_docs=100,
        n_clusters=5,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="ja",
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words is None


def test_language_none_defaults_to_english_for_backward_compat() -> None:
    """A worker called without ``language`` (older API client) keeps the
    legacy English-stopword behaviour — no regression for EN flows."""
    model, _k = _build_online_pipeline(
        n_docs=100,
        n_clusters=5,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language=None,
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words == "english"


def test_language_normalisation_handles_case_and_whitespace() -> None:
    model, _k = _build_online_pipeline(
        n_docs=100,
        n_clusters=5,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language=" EN ",
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words == "english"
