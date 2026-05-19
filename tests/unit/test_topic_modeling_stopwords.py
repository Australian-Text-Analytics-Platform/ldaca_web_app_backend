"""Label-stage CountVectorizer stop_words is language-routed.

BERTopic's clustering stage works on document embeddings (no stopwords
involved), but the per-topic label stage uses a CountVectorizer to pick
"topic-distinguishing" terms via c-TF-IDF. A language-agnostic English
stopword setting on a Chinese corpus is wrong in two ways:

1. It silently keeps every Chinese function word (的 / 是 / 了 / 在 / 我 /
   你) as a candidate, so they dominate every topic label.
2. It looks like stopword filtering is happening when it isn't.

The new behaviour: ``"english"`` only when the resolved language is
English; ``None`` (no filter) otherwise. Future work can wire in
per-language stopword lists at no API change.
"""

from __future__ import annotations

import pytest

from ldaca_wordflow.core.worker_tasks_topic import (
    _bertopic_language_kwarg,
    _build_classic_pipeline,
    _build_label_vectorizer,
    _resolve_top_n_words,
)

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


# ---------------------------------------------------------------------------
# Classic-pipeline + label-vectorizer coverage for the multilingual fix.
# Before the fix, the classic pipeline passed no ``vectorizer_model`` so
# BERTopic defaulted to ``CountVectorizer(stop_words="english")`` whose
# ``\b\w\w+\b`` regex can't segment CJK. After the fix, the classic
# pipeline picks the language-aware vectorizer and forwards
# ``language="multilingual"`` to BERTopic.
# ---------------------------------------------------------------------------


def test_label_vectorizer_english_uses_sklearn_english_stoplist() -> None:
    vec = _build_label_vectorizer("en")
    assert vec.stop_words == "english"


def test_label_vectorizer_chinese_drops_stopwords_and_uses_unicode_word_regex() -> None:
    vec = _build_label_vectorizer("zh")
    assert vec.stop_words is None
    assert vec.token_pattern == r"(?u)\b\w+\b"


def test_label_vectorizer_segments_space_joined_chinese_tokens() -> None:
    """Sanity check: the Unicode-word regex segments pre-tokenised, space-
    joined Chinese into the original tokens. Documents the contract the
    multilingual path relies on — feed BERTopic ``"中文 分词 测试"`` and
    c-TF-IDF sees three distinct words, not one ideograph blob."""
    import re

    vec = _build_label_vectorizer("zh")
    pattern = re.compile(vec.token_pattern)
    assert pattern.findall("中文 分词 测试") == ["中文", "分词", "测试"]


def test_classic_pipeline_attaches_multilingual_vectorizer_for_chinese() -> None:
    model = _build_classic_pipeline(
        min_topic_size=10,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="zh",
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words is None
    assert vec.token_pattern == r"(?u)\b\w+\b"


def test_classic_pipeline_keeps_english_default_for_english() -> None:
    model = _build_classic_pipeline(
        min_topic_size=10,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="en",
    )
    vec = _vectorizer(model)
    assert vec is not None
    assert vec.stop_words == "english"


def test_bertopic_language_kwarg_maps_to_multilingual_for_non_en() -> None:
    assert _bertopic_language_kwarg("en") == "english"
    assert _bertopic_language_kwarg(None) == "english"
    assert _bertopic_language_kwarg("zh") == "multilingual"
    assert _bertopic_language_kwarg("ja") == "multilingual"
    assert _bertopic_language_kwarg("multi") == "multilingual"


# ---------------------------------------------------------------------------
# top_n_words plumbing — guards against the silent-cap regression where
# "Words per topic = 35" only produced 10 candidates because BERTopic's
# top_n_words default was never overridden. With the frontend stopword
# filter on, that left ~1–3 visible words per topic on a CJK run.
# ---------------------------------------------------------------------------


def test_resolve_top_n_words_floors_at_fifty() -> None:
    assert _resolve_top_n_words(0) == 50
    assert _resolve_top_n_words(None) == 50
    assert _resolve_top_n_words(5) == 50
    assert _resolve_top_n_words(20) == 50


def test_resolve_top_n_words_scales_with_user_cap() -> None:
    # 2× headroom so the post-fit stopword filter has enough material
    # to still produce a meaningful slice at the user's chosen cap.
    assert _resolve_top_n_words(30) == 60
    assert _resolve_top_n_words(35) == 70
    assert _resolve_top_n_words(100) == 200


def test_classic_pipeline_forwards_top_n_words_to_bertopic() -> None:
    """Direct ``top_n_words`` attribute check on the built model — guarantees
    the kwarg actually reaches BERTopic, not just our helper signature."""
    model = _build_classic_pipeline(
        min_topic_size=10,
        random_state=42,
        embedder=_make_dummy_embedder(),
        language="zh",
        top_n_words=70,
    )
    assert getattr(model, "top_n_words", None) == 70

