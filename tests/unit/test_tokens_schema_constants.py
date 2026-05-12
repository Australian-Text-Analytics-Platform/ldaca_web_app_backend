"""Phase 2.1: canonical tokens-column schema constants in generated_columns.

Asserts the constants we vend in
``ldaca_web_app.api.workspaces.analyses.generated_columns`` line up
exactly with the schema polars-text's ``tokenize_with_offsets``
actually emits. This is the contract that downstream token-consuming
tools (concordance tokens-mode, token-frequency tokens-path) rely on,
so any drift between Rust and Python schemas must fail loudly here.
"""

from __future__ import annotations

import polars as pl
import polars_text as pt

from ldaca_web_app.api.workspaces.analyses.generated_columns import (
    TOKENS_COLUMN,
    TOKENS_END_FIELD,
    TOKENS_START_FIELD,
    TOKENS_TOKEN_FIELD,
    is_tokens_column,
    tokens_struct_dtype,
    tokens_struct_projection,
)


def test_tokens_column_name() -> None:
    assert TOKENS_COLUMN == "TOKENS_tokens"


def test_struct_field_names_match_rust_output() -> None:
    assert TOKENS_TOKEN_FIELD == "token"
    assert TOKENS_START_FIELD == "start"
    assert TOKENS_END_FIELD == "end"


def test_tokens_struct_dtype_matches_polars_text_output() -> None:
    # Run polars-text and assert the dtype it actually emits matches what
    # our generated_columns constant declares.
    df = pl.DataFrame({"text": ["Hello world"]})
    out = df.select(
        pt.tokenize_with_offsets(pl.col("text")).alias(TOKENS_COLUMN)
    )
    assert out.schema[TOKENS_COLUMN] == tokens_struct_dtype(), (
        f"polars-text emits {out.schema[TOKENS_COLUMN]!r}, "
        f"but generated_columns declares {tokens_struct_dtype()!r}"
    )


def test_is_tokens_column_recognises_canonical_shape() -> None:
    df = pl.DataFrame({"text": ["hi"]})
    out = df.select(
        pt.tokenize_with_offsets(pl.col("text")).alias(TOKENS_COLUMN)
    )
    assert is_tokens_column(TOKENS_COLUMN, out.schema[TOKENS_COLUMN])


def test_is_tokens_column_rejects_wrong_name() -> None:
    df = pl.DataFrame({"text": ["hi"]})
    out = df.select(
        pt.tokenize_with_offsets(pl.col("text")).alias("something_else")
    )
    assert not is_tokens_column("something_else", out.schema["something_else"])


def test_is_tokens_column_rejects_wrong_dtype() -> None:
    # A plain string column with the magic name should NOT be detected.
    df = pl.DataFrame({TOKENS_COLUMN: ["nope"]})
    assert not is_tokens_column(TOKENS_COLUMN, df.schema[TOKENS_COLUMN])


def test_struct_projection_unpacks_fields() -> None:
    df = pl.DataFrame({"text": ["hello world"]})
    tokens_df = df.select(
        pt.tokenize_with_offsets(pl.col("text")).alias(TOKENS_COLUMN)
    ).explode(TOKENS_COLUMN)
    unpacked = tokens_df.select(*tokens_struct_projection())
    assert set(unpacked.columns) == {
        TOKENS_TOKEN_FIELD,
        TOKENS_START_FIELD,
        TOKENS_END_FIELD,
    }
    assert unpacked.height > 0
