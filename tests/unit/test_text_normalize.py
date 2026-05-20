"""Tests for ldaca_wordflow.core.text_normalize.

Pins three contracts:
  1. Real UTF-8 / CP-1252 mojibake gets repaired.
  2. Correctly-encoded non-Latin text (CJK, Arabic, Cyrillic, fullwidth punct,
     CJK quotation marks, halfwidth katakana) passes through untouched —
     this is the multilingual-safety guarantee we configured ftfy for.
  3. ftfy's cost-function safety net leaves ambiguous strings alone rather
     than committing a damaging "repair".
"""

from __future__ import annotations

import polars as pl
import pytest

from ldaca_wordflow.core.text_normalize import (
    _MOJIBAKE_RE,
    repair_mojibake,
    repair_text_columns,
)


class TestRepairMojibake:
    @pytest.mark.parametrize(
        "broken,fixed",
        [
            ("â€œhello worldâ€\x9d", "“hello world”"),
            ("oneâ€\x94two", "one—two"),
            ("so onâ€¦", "so on…"),
            ("Itâ€™s here", "It’s here"),
            ("CafÃ©", "Café"),
            ("Â£10", "£10"),
        ],
    )
    def test_repairs_real_mojibake(self, broken: str, fixed: str) -> None:
        assert repair_mojibake(broken) == fixed

    @pytest.mark.parametrize(
        "clean",
        [
            "",
            "plain ASCII only",
            "Café Mëtàl Ümläüt",  # legitimate accented Latin
            "你好，世界！",  # Simplified Chinese + fullwidth punctuation
            "繁體中文「引用」",  # Traditional Chinese + CJK quotes
            "こんにちは、日本語のテキストです。",  # Japanese mixed scripts
            "ｶﾀｶﾅ",  # halfwidth katakana (would be damaged by fix_character_width)
            "안녕하세요 한국어",  # Korean
            "Здравствуйте, мир!",  # Cyrillic
            "مرحبا بالعالم",  # Arabic
            "10³ × 5²",  # superscripts (would be damaged by NFKC normalisation)
            "« guillemets »",  # French guillemets (would be uncurled)
        ],
    )
    def test_passes_clean_text_through_untouched(self, clean: str) -> None:
        assert repair_mojibake(clean) == clean

    def test_handles_non_string_input(self) -> None:
        assert repair_mojibake(None) is None
        assert repair_mojibake(42) == 42
        assert repair_mojibake(b"bytes") == b"bytes"

    def test_gate_regex_skips_correctly_encoded_non_latin(self) -> None:
        # The pattern gate must NOT match these — they're correctly-encoded
        # UTF-8 and contain no CP-1252 mojibake bytes.
        assert _MOJIBAKE_RE.search("你好，世界") is None
        assert _MOJIBAKE_RE.search("こんにちは") is None
        assert _MOJIBAKE_RE.search("Здравствуйте") is None
        assert _MOJIBAKE_RE.search("مرحبا") is None
        # And it DOES match the classic mojibake signatures
        assert _MOJIBAKE_RE.search("â€™") is not None
        assert _MOJIBAKE_RE.search("Ã©") is not None
        assert _MOJIBAKE_RE.search("Â£") is not None


class TestRepairTextColumns:
    def test_repairs_strings_in_lazyframe(self) -> None:
        lf = pl.LazyFrame(
            {
                "text": ["CafÃ©", "Itâ€™s here", "你好"],
                "n": [1, 2, 3],
            }
        )
        out = repair_text_columns(lf).collect()
        assert out["text"].to_list() == ["Café", "It’s here", "你好"]
        assert out["n"].to_list() == [1, 2, 3]

    def test_repairs_strings_in_dataframe(self) -> None:
        df = pl.DataFrame({"text": ["CafÃ©", "你好"], "n": [1, 2]})
        out = repair_text_columns(df)
        assert isinstance(out, pl.DataFrame)
        assert out["text"].to_list() == ["Café", "你好"]

    def test_leaves_non_string_columns_alone(self) -> None:
        df = pl.DataFrame(
            {
                "text": ["CafÃ©"],
                "count": [42],
                "ratio": [3.14],
                "flag": [True],
            }
        )
        out = repair_text_columns(df)
        assert isinstance(out, pl.DataFrame)
        assert out.schema["count"] == pl.Int64
        assert out.schema["ratio"] == pl.Float64
        assert out.schema["flag"] == pl.Boolean
        assert out["text"].to_list() == ["Café"]

    def test_no_string_columns_returns_frame_unchanged(self) -> None:
        df = pl.DataFrame({"n": [1, 2], "r": [1.5, 2.5]})
        assert repair_text_columns(df).equals(df)

    def test_explicit_column_filter(self) -> None:
        df = pl.DataFrame({"a": ["CafÃ©"], "b": ["CafÃ©"]})
        out = repair_text_columns(df, columns=["a"])
        assert isinstance(out, pl.DataFrame)
        assert out["a"].to_list() == ["Café"]
        # 'b' was not in the filter, so it must stay broken
        assert out["b"].to_list() == ["CafÃ©"]
