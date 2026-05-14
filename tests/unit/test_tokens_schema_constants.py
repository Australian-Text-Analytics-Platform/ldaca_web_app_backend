"""Phase 2.1 v2: derived-column naming helper + tokens schema contract.

Asserts:
- ``derived_column_name`` / ``parse_derived_column`` round-trip,
- ``tokens_struct_dtype()`` lines up with the schema polars-text's
  ``tokenize_with_offsets`` actually emits, and
- ``is_derived_tokens_column`` reads from ``Node.derived`` (the metadata
  index per decision 7), not from a fixed magic column name.

These are the contracts every Phase 2 consumer (concordance tokens-mode,
token-frequency tokens-path, future POS) relies on, so any drift between
Rust and Python schemas — or between the naming helper and its consumers
— must fail loudly here.
"""

from __future__ import annotations

import polars as pl
import polars_text as pt
from docworkspace import Node

from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    DERIVED_PREFIX,
    TOKENS_END_FIELD,
    TOKENS_FORM,
    TOKENS_START_FIELD,
    TOKENS_TOKEN_FIELD,
    derived_column_name,
    is_derived_column_name,
    is_derived_tokens_column,
    parse_derived_column,
    tokens_struct_dtype,
    tokens_struct_projection,
)


# Test fixture: canonical (source, model) we use throughout this module.
_TEXT_COLUMN = "text"
_BERT_MODEL = "bert-base-uncased"
_TOKENS_NAME = f"{DERIVED_PREFIX}.{TOKENS_FORM}.{_TEXT_COLUMN}.{_BERT_MODEL}"


def test_derived_column_name_builds_canonical_label() -> None:
    assert derived_column_name(TOKENS_FORM, _TEXT_COLUMN, _BERT_MODEL) == _TOKENS_NAME
    assert derived_column_name("pos", "body", "spacy-en") == "__derived__.pos.body.spacy-en"


def test_parse_derived_column_round_trips() -> None:
    assert parse_derived_column(_TOKENS_NAME) == (TOKENS_FORM, _TEXT_COLUMN, _BERT_MODEL)
    assert parse_derived_column("__derived__.pos.body.spacy-en") == (
        "pos",
        "body",
        "spacy-en",
    )


def test_parse_derived_column_rejects_non_derived_names() -> None:
    assert parse_derived_column("plain_column") is None
    # Missing prefix.
    assert parse_derived_column("tokens.text.jieba") is None
    # Wrong number of parts (source or model containing dots is ambiguous —
    # by design we treat it as unparseable; consult Node.derived instead).
    assert parse_derived_column("__derived__.tokens.foo") is None
    assert parse_derived_column("__derived__.tokens.foo.bar.baz") is None


def test_struct_field_names_match_rust_output() -> None:
    assert TOKENS_TOKEN_FIELD == "token"
    assert TOKENS_START_FIELD == "start"
    assert TOKENS_END_FIELD == "end"


def test_tokens_struct_dtype_matches_polars_text_output() -> None:
    df = pl.DataFrame({"text": ["Hello world"]})
    out = df.select(pt.tokenize_with_offsets(pl.col("text")).alias(_TOKENS_NAME))
    assert out.schema[_TOKENS_NAME] == tokens_struct_dtype(), (
        f"polars-text emits {out.schema[_TOKENS_NAME]!r}, "
        f"but generated_columns declares {tokens_struct_dtype()!r}"
    )


def test_is_derived_tokens_column_reads_from_node_metadata() -> None:
    # Build a node with a derived tokens column registered in Node.derived.
    df = pl.DataFrame({"text": ["hi"]})
    with_tokens = df.lazy().select(
        pl.col("text"), pt.tokenize_with_offsets(pl.col("text")).alias(_TOKENS_NAME)
    )
    node = Node(data=with_tokens, name="tokens_root")
    node.register_derived_column(
        _TOKENS_NAME,
        {  # type: ignore[arg-type]
            "source_column": _TEXT_COLUMN,
            "form": TOKENS_FORM,
            "model": _BERT_MODEL,
            "language": "en",
            "generated_at": "2026-05-12T00:00:00+00:00",
        },
    )
    assert is_derived_tokens_column(node, _TOKENS_NAME)


def test_is_derived_tokens_column_rejects_unregistered_column() -> None:
    df = pl.DataFrame({"text": ["hi"]})
    out = df.lazy().select(
        pt.tokenize_with_offsets(pl.col("text")).alias(_TOKENS_NAME)
    )
    node = Node(data=out, name="unregistered")
    # Column exists in schema but isn't in Node.derived → not a tokens column.
    assert not is_derived_tokens_column(node, _TOKENS_NAME)


def test_is_derived_tokens_column_rejects_wrong_form() -> None:
    df = pl.DataFrame({"text": ["hi"]}).lazy()
    pos_name = "__derived__.pos.text.spacy-en"
    node = Node(data=df, name="pos_root")
    node.register_derived_column(
        pos_name,
        {  # type: ignore[arg-type]
            "source_column": _TEXT_COLUMN,
            "form": "pos",
            "model": "spacy-en",
            "language": "en",
            "generated_at": "2026-05-12T00:00:00+00:00",
        },
    )
    assert not is_derived_tokens_column(node, pos_name)


def test_is_derived_column_name_prefix_check() -> None:
    assert is_derived_column_name(_TOKENS_NAME)
    assert is_derived_column_name("__derived__.pos.text.spacy-en")
    assert not is_derived_column_name("text")
    assert not is_derived_column_name("TOKENS_tokens")  # v1 legacy name


def test_tokens_struct_projection_unpacks_fields() -> None:
    df = pl.DataFrame({"text": ["hello world"]})
    tokens_df = df.select(
        pt.tokenize_with_offsets(pl.col("text")).alias(_TOKENS_NAME)
    ).explode(_TOKENS_NAME)
    unpacked = tokens_df.select(*tokens_struct_projection(_TOKENS_NAME))
    assert set(unpacked.columns) == {
        TOKENS_TOKEN_FIELD,
        TOKENS_START_FIELD,
        TOKENS_END_FIELD,
    }
    assert unpacked.height > 0
