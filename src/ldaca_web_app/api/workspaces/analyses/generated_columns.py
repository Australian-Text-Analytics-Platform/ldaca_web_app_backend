"""Canonical generated analysis column names.

Used by:
- concordance, topic-modeling, and quotation analysis routes/workers

Why:
- Keeps generated column naming consistent across live results, detach flows,
  worker artifacts, and frontend-facing payloads.
"""

from __future__ import annotations

import polars as pl

CONC_LEFT_CONTEXT_COLUMN = "CONC_left_context"
CONC_MATCHED_TEXT_COLUMN = "CONC_matched_text"
CONC_RIGHT_CONTEXT_COLUMN = "CONC_right_context"
CONC_START_IDX_COLUMN = "CONC_start_idx"
CONC_END_IDX_COLUMN = "CONC_end_idx"
CONC_L1_COLUMN = "CONC_l1"
CONC_R1_COLUMN = "CONC_r1"
CONC_L1_FREQ_COLUMN = "CONC_l1_freq"
CONC_R1_FREQ_COLUMN = "CONC_r1_freq"
# Per-hit raw-window stitch. Same slicing rule the dispersion-detach
# aggregation uses for the per-document join, exposed per-hit so the user
# can opt the column into both the table view and the per-hit detach.
CONC_EXTRACTION_COLUMN = "CONC_extraction"

CORE_CONCORDANCE_COLUMNS = (
    CONC_LEFT_CONTEXT_COLUMN,
    CONC_MATCHED_TEXT_COLUMN,
    CONC_RIGHT_CONTEXT_COLUMN,
    CONC_START_IDX_COLUMN,
    CONC_END_IDX_COLUMN,
    CONC_L1_COLUMN,
    CONC_R1_COLUMN,
)

MATERIALIZED_CONCORDANCE_COLUMNS = CORE_CONCORDANCE_COLUMNS + (
    CONC_L1_FREQ_COLUMN,
    CONC_R1_FREQ_COLUMN,
)


def concordance_extraction_expr(document_column: str) -> pl.Expr:
    """Polars expression that slices the raw KWIC window from ``document_column``.

    Goes from the start of ``CONC_left_context`` to the end of
    ``CONC_right_context`` so the extract preserves the original whitespace
    and punctuation between the context tokens and the matched span.
    ``polars_text.concordance`` returns context strings token-bounded with no
    surrounding whitespace, so we assume a single-character separator between
    each context and the matched span — accurate for whitespace-tokenised
    text, off by a small amount only when the separator is multi-char (e.g.
    em-dashes or multiple spaces).

    Requires the input frame to carry ``CONC_left_context``,
    ``CONC_right_context``, ``CONC_start_idx``, ``CONC_end_idx``, and the
    named ``document_column``.
    """
    left_len = pl.col(CONC_LEFT_CONTEXT_COLUMN).fill_null("").str.len_chars()
    right_len = pl.col(CONC_RIGHT_CONTEXT_COLUMN).fill_null("").str.len_chars()
    left_sep = (
        pl.when(left_len > 0)
        .then(pl.lit(1, dtype=pl.Int64))
        .otherwise(pl.lit(0, dtype=pl.Int64))
    )
    right_sep = (
        pl.when(right_len > 0)
        .then(pl.lit(1, dtype=pl.Int64))
        .otherwise(pl.lit(0, dtype=pl.Int64))
    )
    window_start = pl.max_horizontal(
        pl.lit(0, dtype=pl.Int64),
        pl.col(CONC_START_IDX_COLUMN) - left_len - left_sep,
    )
    window_end = pl.col(CONC_END_IDX_COLUMN) + right_len + right_sep
    return (
        pl.col(document_column)
        .cast(pl.Utf8, strict=False)
        .str.slice(window_start, window_end - window_start)
        .alias(CONC_EXTRACTION_COLUMN)
    )


def compute_concordance_extraction_string(
    *,
    document_text: str,
    left_context: str | None,
    right_context: str | None,
    start_idx: int,
    end_idx: int,
) -> str:
    """Python equivalent of ``concordance_extraction_expr`` for one hit.

    Used by the live (non-materialised) per-page response builder where each
    hit is projected row-by-row from a struct list rather than batched
    through Polars expressions.
    """
    left = left_context or ""
    right = right_context or ""
    left_sep = 1 if left else 0
    right_sep = 1 if right else 0
    window_start = max(0, int(start_idx) - len(left) - left_sep)
    window_end = int(end_idx) + len(right) + right_sep
    return (document_text or "")[window_start:window_end]


def concordance_struct_projection(struct_column: str) -> tuple[pl.Expr, ...]:
    """Project raw concordance struct fields into canonical prefixed columns."""
    return (
        pl.col(struct_column)
        .struct.field("left_context")
        .alias(CONC_LEFT_CONTEXT_COLUMN),
        pl.col(struct_column)
        .struct.field("matched_text")
        .alias(CONC_MATCHED_TEXT_COLUMN),
        pl.col(struct_column)
        .struct.field("right_context")
        .alias(CONC_RIGHT_CONTEXT_COLUMN),
        pl.col(struct_column).struct.field("start_idx").alias(CONC_START_IDX_COLUMN),
        pl.col(struct_column).struct.field("end_idx").alias(CONC_END_IDX_COLUMN),
        pl.col(struct_column).struct.field("l1").alias(CONC_L1_COLUMN),
        pl.col(struct_column).struct.field("r1").alias(CONC_R1_COLUMN),
    )


TOPIC_COLUMN = "TOPIC_topic"
TOPIC_MEANING_COLUMN = "TOPIC_topic_meaning"

QUOTE_EXTRACTION_COLUMN = "QUOTE_extraction"
QUOTE_SPEAKER_COLUMN = "QUOTE_speaker"
QUOTE_SPEAKER_START_IDX_COLUMN = "QUOTE_speaker_start_idx"
QUOTE_SPEAKER_END_IDX_COLUMN = "QUOTE_speaker_end_idx"
QUOTE_QUOTE_COLUMN = "QUOTE_quote"
QUOTE_QUOTE_START_IDX_COLUMN = "QUOTE_quote_start_idx"
QUOTE_QUOTE_END_IDX_COLUMN = "QUOTE_quote_end_idx"
QUOTE_VERB_COLUMN = "QUOTE_verb"
QUOTE_VERB_START_IDX_COLUMN = "QUOTE_verb_start_idx"
QUOTE_VERB_END_IDX_COLUMN = "QUOTE_verb_end_idx"
QUOTE_TYPE_COLUMN = "QUOTE_quote_type"
QUOTE_TOKEN_COUNT_COLUMN = "QUOTE_quote_token_count"
QUOTE_IS_FLOATING_COLUMN = "QUOTE_is_floating_quote"
QUOTE_ROW_IDX_COLUMN = "QUOTE_quote_row_idx"

QUOTE_COLUMN_NAMES = (
    QUOTE_SPEAKER_COLUMN,
    QUOTE_SPEAKER_START_IDX_COLUMN,
    QUOTE_SPEAKER_END_IDX_COLUMN,
    QUOTE_QUOTE_COLUMN,
    QUOTE_QUOTE_START_IDX_COLUMN,
    QUOTE_QUOTE_END_IDX_COLUMN,
    QUOTE_VERB_COLUMN,
    QUOTE_VERB_START_IDX_COLUMN,
    QUOTE_VERB_END_IDX_COLUMN,
    QUOTE_TYPE_COLUMN,
    QUOTE_TOKEN_COUNT_COLUMN,
    QUOTE_IS_FLOATING_COLUMN,
    QUOTE_ROW_IDX_COLUMN,
)


def quotation_struct_projection(struct_column: str) -> tuple[pl.Expr, ...]:
    """Project raw quotation struct fields into canonical prefixed columns."""
    return (
        pl.col(struct_column).struct.field("speaker").alias(QUOTE_SPEAKER_COLUMN),
        pl.col(struct_column)
        .struct.field("speaker_start_idx")
        .alias(QUOTE_SPEAKER_START_IDX_COLUMN),
        pl.col(struct_column)
        .struct.field("speaker_end_idx")
        .alias(QUOTE_SPEAKER_END_IDX_COLUMN),
        pl.col(struct_column).struct.field("quote").alias(QUOTE_QUOTE_COLUMN),
        pl.col(struct_column)
        .struct.field("quote_start_idx")
        .alias(QUOTE_QUOTE_START_IDX_COLUMN),
        pl.col(struct_column)
        .struct.field("quote_end_idx")
        .alias(QUOTE_QUOTE_END_IDX_COLUMN),
        pl.col(struct_column).struct.field("verb").alias(QUOTE_VERB_COLUMN),
        pl.col(struct_column)
        .struct.field("verb_start_idx")
        .alias(QUOTE_VERB_START_IDX_COLUMN),
        pl.col(struct_column)
        .struct.field("verb_end_idx")
        .alias(QUOTE_VERB_END_IDX_COLUMN),
        pl.col(struct_column).struct.field("quote_type").alias(QUOTE_TYPE_COLUMN),
        pl.col(struct_column)
        .struct.field("quote_token_count")
        .alias(QUOTE_TOKEN_COUNT_COLUMN),
        pl.col(struct_column)
        .struct.field("is_floating_quote")
        .alias(QUOTE_IS_FLOATING_COLUMN),
        pl.col(struct_column).struct.field("quote_row_idx").alias(QUOTE_ROW_IDX_COLUMN),
    )
