"""Phase 3 foundation: ``effective_language`` resolves the language a
tool should use, with a stable precedence:

1. Explicit ``request_language`` wins,
2. else first non-empty language on a derived column,
3. else default ``"en"`` so existing English flows stay unchanged.

``UnsupportedLanguageError`` carries structured fields so the API layer
can render typed errors without parsing the message string.
"""

from __future__ import annotations

import polars as pl
import pytest
from docworkspace import Node

from ldaca_web_app.core.i18n import (
    DEFAULT_LANGUAGE,
    UnsupportedLanguageError,
    effective_language,
    require_language,
)


def _node_with_derived(language: str) -> Node:
    df = pl.DataFrame({"text": ["a"]}).lazy()
    node = Node(data=df, name="root")
    node.register_derived_column(
        "__derived__.tokens.text.jieba",
        {  # type: ignore[arg-type]
            "source_column": "text",
            "form": "tokens",
            "model": "jieba",
            "language": language,
            "generated_at": "2026-05-12T00:00:00+00:00",
        },
    )
    return node


def test_effective_language_request_wins_over_derived() -> None:
    node = _node_with_derived("zh")
    assert effective_language("ja", node) == "ja"


def test_effective_language_falls_back_to_derived_when_request_empty() -> None:
    node = _node_with_derived("zh")
    assert effective_language(None, node) == "zh"
    assert effective_language("", node) == "zh"
    assert effective_language("   ", node) == "zh"


def test_effective_language_defaults_to_english_with_nothing_set() -> None:
    df = pl.DataFrame({"text": ["a"]}).lazy()
    node = Node(data=df, name="plain")
    assert effective_language(None, node) == DEFAULT_LANGUAGE
    assert effective_language(None, None) == DEFAULT_LANGUAGE


def test_effective_language_normalises_case_and_whitespace() -> None:
    assert effective_language("ZH", None) == "zh"
    assert effective_language(" En ", None) == "en"


def test_require_language_accepts_supported() -> None:
    # No exception → passes through.
    require_language("Quotation extractor", "en")
    require_language("Other tool", "zh", supported=("zh", "ja"))


def test_require_language_raises_unsupported() -> None:
    with pytest.raises(UnsupportedLanguageError) as exc_info:
        require_language("Quotation extractor", "zh")
    err = exc_info.value
    assert err.tool == "Quotation extractor"
    assert err.language == "zh"
    assert "English-only" in str(err)


def test_unsupported_language_error_accepts_custom_message() -> None:
    err = UnsupportedLanguageError("X", "ja", message="custom reason")
    assert str(err) == "custom reason"
    assert err.tool == "X"
    assert err.language == "ja"
