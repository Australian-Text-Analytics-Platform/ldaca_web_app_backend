"""Bug 4 regression: ``whole_word`` toggle must be a no-op on CJK nodes.

``\\b``-style word boundary semantics don't apply to Chinese / Japanese /
Korean text — there's no whitespace separator between morphemes. Pre-fix,
ticking the toggle on the UI forced the backend to wrap every node's
pattern in ``\\b(?:…)\\b``, which produces zero hits on CJK corpora and
silently breaks mixed EN + CJK selections by killing the CJK side.

The fix lives in :func:`build_concordance_search_pattern` which now takes
an optional ``language`` kwarg and skips the ``\\b`` wrap when the node
is CJK. Per-node iteration in :func:`compute_node_concordance_page`
injects the resolved language as ``node_language`` into a per-node copy
of the request dict, so EN nodes in the same selection keep their
whole-word behaviour.
"""

from __future__ import annotations

import re
from typing import Any

import pytest
from ldaca_wordflow.api.workspaces.analyses.concordance_core import (
    _whole_word_active_for_language,
    build_concordance_lazyframe,
    build_concordance_search_pattern,
)

# ---------------------------------------------------------------------------
# Pure-function tests for the language-aware pattern builder
# ---------------------------------------------------------------------------


class TestBuildSearchPattern:
    def test_no_whole_word_returns_pattern_unchanged(self):
        assert build_concordance_search_pattern(
            "hello", regex=False, whole_word=False
        ) == ("hello", False)

    def test_whole_word_en_wraps_pattern_with_word_boundaries(self):
        pattern, use_regex = build_concordance_search_pattern(
            "hello", regex=False, whole_word=True, language="en"
        )
        assert use_regex is True
        # ``re.escape("hello")`` is ``"hello"`` so the wrap is verbatim.
        assert pattern == r"\b(?:hello)\b"

    def test_whole_word_en_with_regex_keeps_pattern_as_regex(self):
        pattern, use_regex = build_concordance_search_pattern(
            r"equ\w*", regex=True, whole_word=True, language="en"
        )
        assert use_regex is True
        assert pattern == r"\b(?:equ\w*)\b"

    def test_whole_word_suppressed_for_japanese(self):
        pattern, use_regex = build_concordance_search_pattern(
            "中", regex=False, whole_word=True, language="ja"
        )
        # The toggle is silently dropped on JA — falls through to the plain
        # pattern + the user's original regex flag.
        assert pattern == "中"
        assert use_regex is False

    @pytest.mark.parametrize("language", ["zh", "ja", "ko", "JA", "Zh", "  ko  "])
    def test_whole_word_suppressed_for_all_cjk_codes_and_normalisation(
        self, language: str
    ):
        pattern, _ = build_concordance_search_pattern(
            "x", regex=False, whole_word=True, language=language
        )
        # No \b wrap regardless of case / surrounding whitespace.
        assert pattern == "x"

    def test_whole_word_kept_for_non_cjk_languages(self):
        for lang in ("en", "es", "fr", "de", "multi", "fallback"):
            pattern, _ = build_concordance_search_pattern(
                "x", regex=False, whole_word=True, language=lang
            )
            assert pattern == r"\b(?:x)\b", f"expected wrap for language={lang!r}"

    def test_language_none_keeps_legacy_behaviour(self):
        # When no language is known the historical EN-style behaviour wins
        # — pattern gets wrapped.
        pattern, _ = build_concordance_search_pattern(
            "x", regex=False, whole_word=True, language=None
        )
        assert pattern == r"\b(?:x)\b"


class TestWholeWordActiveForLanguage:
    def test_false_when_request_off_regardless_of_language(self):
        for lang in (None, "en", "ja", "zh", "ko"):
            assert _whole_word_active_for_language(False, lang) is False, (
                f"expected False for language={lang!r}"
            )

    def test_false_for_cjk_when_request_on(self):
        for lang in ("ja", "zh", "ko"):
            assert _whole_word_active_for_language(True, lang) is False

    def test_true_for_non_cjk_when_request_on(self):
        for lang in (None, "en", "es", "fr", "de", "multi"):
            assert _whole_word_active_for_language(True, lang) is True


# ---------------------------------------------------------------------------
# Lazy-plan integration test — verifies build_concordance_lazyframe reads
# ``node_language`` from the request dict (the field per-node iteration in
# compute_node_concordance_page injects).
# ---------------------------------------------------------------------------


class TestLazyframeReadsNodeLanguage:
    def test_request_node_language_drives_pattern_choice(self, monkeypatch):
        """``build_concordance_lazyframe`` forwards the request's
        ``node_language`` to the pattern builder. We spy on the builder
        rather than inspecting the resulting lazy plan because polars
        ``.explain()`` doesn't surface plugin kwargs.
        """
        import polars as pl
        from ldaca_wordflow.api.workspaces.analyses import concordance_core

        captured: list[dict[str, object]] = []

        def fake_build_pattern(search_word, *, regex, whole_word, language=None):
            captured.append(
                {
                    "search_word": search_word,
                    "regex": regex,
                    "whole_word": whole_word,
                    "language": language,
                }
            )
            return build_concordance_search_pattern(
                search_word,
                regex=regex,
                whole_word=whole_word,
                language=language,
            )

        monkeypatch.setattr(
            concordance_core,
            "build_concordance_search_pattern",
            fake_build_pattern,
        )

        df = pl.DataFrame({"text": ["a b c"]}).lazy()
        request = {
            "search_word": "b",
            "num_left_tokens": 1,
            "num_right_tokens": 1,
            "regex": False,
            "whole_word": True,
            "case_sensitive": False,
            "node_language": "ja",
        }
        build_concordance_lazyframe(df, "text", request)

        assert len(captured) == 1
        assert captured[0]["language"] == "ja"
        assert captured[0]["whole_word"] is True

    def test_node_language_takes_precedence_over_global_language(self, monkeypatch):
        """``node_language`` injected by the per-node iteration wins over
        ``language`` (the request-wide hint). EN globally + JA per-node →
        the pattern builder sees ``language="ja"``."""
        import polars as pl
        from ldaca_wordflow.api.workspaces.analyses import concordance_core

        seen: dict[str, object] = {}

        def fake_build_pattern(search_word, *, regex, whole_word, language=None):
            seen["language"] = language
            return build_concordance_search_pattern(
                search_word,
                regex=regex,
                whole_word=whole_word,
                language=language,
            )

        monkeypatch.setattr(
            concordance_core,
            "build_concordance_search_pattern",
            fake_build_pattern,
        )

        df = pl.DataFrame({"text": ["a"]}).lazy()
        build_concordance_lazyframe(
            df,
            "text",
            {
                "search_word": "a",
                "num_left_tokens": 1,
                "num_right_tokens": 1,
                "regex": False,
                "whole_word": True,
                "case_sensitive": False,
                "language": "en",
                "node_language": "ja",
            },
        )

        assert seen["language"] == "ja"


# ---------------------------------------------------------------------------
# Worker-task integration: pass language=ja, whole_word=True → pattern
# is NOT wrapped before reaching the concordance plugin.
# ---------------------------------------------------------------------------


class TestWorkerDataframeRespectsLanguage:
    def test_dataframe_builder_no_wrap_on_ja(self, monkeypatch):
        """`_build_concordance_occurrence_dataframe(language="ja",
        whole_word=True)` must call build_concordance_search_pattern with a
        path that produces the raw pattern (no \\b)."""
        from ldaca_wordflow.core import worker_tasks_concordance as wtc

        captured: dict[str, Any] = {}

        def fake_build_pattern(search_word, *, regex, whole_word, language=None):
            pattern, use_regex = build_concordance_search_pattern(
                search_word,
                regex=regex,
                whole_word=whole_word,
                language=language,
            )
            captured["result"] = (pattern, use_regex)
            captured["language_seen"] = language
            return pattern, use_regex

        monkeypatch.setattr(wtc, "build_concordance_search_pattern", fake_build_pattern)

        wtc._build_concordance_occurrence_dataframe(
            node_corpus=["今日は良い天気です"],
            document_column="document",
            search_word="今日",
            num_left_tokens=1,
            num_right_tokens=1,
            regex=False,
            whole_word=True,
            case_sensitive=False,
            include_document_column=True,
            extra_columns_data=None,
            language="ja",
        )

        assert captured["language_seen"] == "ja"
        pattern, _ = captured["result"]
        # JA + whole_word → no \b wrap
        assert pattern == "今日"
        assert not re.search(r"\\b", pattern)

    def test_dataframe_builder_wraps_on_en(self, monkeypatch):
        from ldaca_wordflow.core import worker_tasks_concordance as wtc

        captured: dict[str, str] = {}

        def fake_build_pattern(search_word, *, regex, whole_word, language=None):
            pattern, use_regex = build_concordance_search_pattern(
                search_word,
                regex=regex,
                whole_word=whole_word,
                language=language,
            )
            captured["pattern"] = pattern
            return pattern, use_regex

        monkeypatch.setattr(wtc, "build_concordance_search_pattern", fake_build_pattern)

        wtc._build_concordance_occurrence_dataframe(
            node_corpus=["alpha beta gamma"],
            document_column="document",
            search_word="beta",
            num_left_tokens=1,
            num_right_tokens=1,
            regex=False,
            whole_word=True,
            case_sensitive=False,
            include_document_column=True,
            extra_columns_data=None,
            language="en",
        )

        assert captured["pattern"] == r"\b(?:beta)\b"
