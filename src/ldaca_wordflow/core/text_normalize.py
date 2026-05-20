"""Mojibake repair for loaded corpus text, configured safely for multilingual data.

Why this exists
---------------
Corpus files frequently arrive carrying UTF-8 → CP-1252 → UTF-8 double-encoding
("mojibake"). The smoking gun is sequences like ``â€œ`` for a left curly quote,
``Ã©`` for ``é``, ``Â£`` for ``£``. ftfy fixes that. But ``ftfy.fix_text()``
defaults bundle the mojibake repair with several Latin-centric cleanups that
**mutate legitimate non-Latin text**:

  - ``fix_character_width`` collapses fullwidth CJK punctuation (``，。「」``) to
    ASCII, and damages halfwidth Katakana.
  - ``uncurl_quotes`` strips typography that is meaningful in many writing
    systems (French guillemets, German low-9 quotes, CJK 「」, etc.).
  - ``normalization="NFKC"`` collapses semantically distinct codepoints
    (``10³`` → ``103``, fullwidth-A → ASCII-A).
  - ``decode_inconsistent_utf8`` is flagged "high risk" by upstream itself.

The config below enables ONLY the mojibake-related fixers and pairs them with a
cheap regex pre-filter so the common case (clean text, including CJK / Arabic /
Cyrillic) skips the ftfy call entirely.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Iterable

import polars as pl

try:
    import ftfy
    from ftfy import TextFixerConfig

    _FTFY_AVAILABLE = True
except ImportError:
    _FTFY_AVAILABLE = False
    ftfy = None  # type: ignore[assignment]
    TextFixerConfig = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


# Mojibake repair only; everything language-sensitive is explicitly OFF.
# See module docstring for why each toggle is set the way it is.
_FTFY_CONFIG: Any = (
    TextFixerConfig(
        fix_encoding=True,
        fix_c1_controls=True,
        fix_surrogates=True,
        fix_latin_ligatures=False,
        fix_character_width=False,
        uncurl_quotes=False,
        normalization="NFC",
        unescape_html=False,
        decode_inconsistent_utf8=False,
        restore_byte_a0=False,
    )
    if _FTFY_AVAILABLE
    else None
)


# Pattern gate: classic UTF-8 → CP-1252 mojibake signatures.
#   ``â€``     — leading bytes for curly quotes, en/em dash, ellipsis
#               (UTF-8 E2 80 xx misread as Latin-1 ``â € xx``).
#   ``Ã[\x80-\xbf]`` — accented Latin (é=C3 A9 → Ã©, ñ=C3 B1 → Ã±, …).
#   ``Â[\xa0-\xbf]`` — Latin-1 supplement (£=C2 A3 → Â£, ©=C2 A9 → Â©, …).
#
# Correctly-encoded CJK / Arabic / Cyrillic byte patterns do NOT match this
# regex, which keeps the per-cell cost down to a single ``re.search`` for the
# 99% case where the text is clean.
_MOJIBAKE_RE = re.compile(r"â€|Ã[\x80-\xbf]|Â[\xa0-\xbf]")


def repair_mojibake(text: Any) -> Any:
    """Repair UTF-8/CP-1252 round-trip mojibake in a single value.

    Returns the input unchanged for non-strings, empty strings, or strings
    without mojibake signatures. ftfy's own cost function provides a second
    safety net even on strings that do match the gate regex — if the "fixed"
    candidate scores lower than the original, ftfy leaves it alone.
    """
    if not _FTFY_AVAILABLE or not isinstance(text, str) or not text:
        return text
    if not _MOJIBAKE_RE.search(text):
        return text
    assert ftfy is not None  # for type checkers; _FTFY_AVAILABLE gates it
    return ftfy.fix_text(text, config=_FTFY_CONFIG)


def repair_text_columns(
    frame: pl.DataFrame | pl.LazyFrame,
    columns: Iterable[str] | None = None,
) -> pl.DataFrame | pl.LazyFrame:
    """Apply mojibake repair to every Utf8 column of a Polars frame.

    No-op when ftfy isn't installed. Numeric / temporal / boolean columns pass
    through unchanged. When ``columns`` is provided, only those names are
    touched (and silently skipped if absent or non-string).

    For LazyFrames the repair lives inside the plan and runs at collect time;
    the gate regex keeps the cost negligible for clean text.
    """
    if not _FTFY_AVAILABLE:
        return frame
    schema = (
        frame.collect_schema() if isinstance(frame, pl.LazyFrame) else frame.schema
    )
    requested = set(columns) if columns is not None else None
    string_cols = [
        name
        for name, dtype in schema.items()
        if dtype == pl.String and (requested is None or name in requested)
    ]
    if not string_cols:
        return frame
    return frame.with_columns(
        [
            pl.col(name)
            .map_elements(repair_mojibake, return_dtype=pl.String)
            .alias(name)
            for name in string_cols
        ]
    )
