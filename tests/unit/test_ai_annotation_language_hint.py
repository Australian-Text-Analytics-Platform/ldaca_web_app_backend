"""Phase 3.7: AI annotation prompt carries a language hint.

The corpus language is surfaced to the LLM so it doesn't treat CJK
characters as encoding noise. English flows are byte-identical — the
hint line is only added for non-default languages.
"""

from __future__ import annotations

from ldaca_web_app.api.workspaces.analyses.ai_annotation_core import (
    _build_system_prompt,
)


CLASSES = [
    {"name": "positive", "description": "Positive sentiment"},
    {"name": "negative", "description": "Negative sentiment"},
]


def test_prompt_omits_language_line_for_english_default() -> None:
    prompt = _build_system_prompt(CLASSES, examples=None, language="en")
    assert "Texts to classify are in" not in prompt


def test_prompt_omits_language_line_when_unspecified() -> None:
    """Backward compat: explicit ``language=None`` matches the v1
    English-only prompt byte-for-byte (modulo the new optional kwarg).
    """
    baseline = _build_system_prompt(CLASSES, examples=None)
    with_explicit_none = _build_system_prompt(
        CLASSES, examples=None, language=None
    )
    assert baseline == with_explicit_none
    assert "Texts to classify are in" not in baseline


def test_prompt_adds_language_label_for_chinese() -> None:
    prompt = _build_system_prompt(CLASSES, examples=None, language="zh")
    assert "Texts to classify are in Chinese." in prompt


def test_prompt_adds_language_label_for_japanese() -> None:
    prompt = _build_system_prompt(CLASSES, examples=None, language="ja")
    assert "Texts to classify are in Japanese." in prompt


def test_prompt_falls_back_to_raw_code_for_unknown_language() -> None:
    """A language code we don't have a label for should still propagate as
    a hint — the LLM understands ISO codes well enough."""
    prompt = _build_system_prompt(CLASSES, examples=None, language="xx")
    assert "Texts to classify are in xx." in prompt


def test_prompt_includes_examples_block_alongside_language_hint() -> None:
    examples = [{"query": "好极了", "classification": "positive"}]
    prompt = _build_system_prompt(CLASSES, examples=examples, language="zh")
    assert "Texts to classify are in Chinese." in prompt
    assert "Examples:" in prompt
    assert "好极了" in prompt


def test_prompt_normalises_case_only_for_english_check() -> None:
    """``language="EN"`` should still hit the English-default short-circuit
    (no hint line); the lowercase comparison matches what the resolver
    returns."""
    prompt = _build_system_prompt(CLASSES, examples=None, language="EN")
    assert "Texts to classify are in" not in prompt
