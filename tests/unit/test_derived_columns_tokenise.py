"""Phase 2.3: ``tokenise_column`` adds/replaces a derived tokens column on
the source node and keeps ``Node.derived`` in sync.

Decision 7 design check — the operation mutates the source node's LazyFrame
plan (no child node), is idempotent on ``(source_column, model)``, and the
prior plan lands on the undo stack.
"""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest
from docworkspace import Node

from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    DERIVED_PREFIX,
    TOKENS_FORM,
    derived_column_name,
)
from ldaca_wordflow.core.derived_columns import tokenise_column


def _make_node(name: str = "root") -> Node:
    df = pl.DataFrame(
        {
            "text": ["Hello world", "Goodbye world", "Hello again"],
            "value": [1, 2, 3],
        }
    ).lazy()
    return Node(data=df, name=name)


def test_tokenise_adds_derived_column_and_registers_metadata() -> None:
    node = _make_node()
    expected_name = derived_column_name(TOKENS_FORM, "text", "bert-base-uncased")

    result_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
    )

    assert result_name == expected_name
    assert expected_name in node.data.collect_schema().names()
    assert expected_name in node.derived
    meta = node.derived[expected_name]
    assert meta["source_column"] == "text"
    assert meta["form"] == TOKENS_FORM
    assert meta["model"] == "bert-base-uncased"
    assert meta["language"] == "en"
    assert meta["generated_at"]  # ISO timestamp string

    # The original user-facing columns are still there alongside the derived.
    schema_names = node.data.collect_schema().names()
    assert "text" in schema_names
    assert "value" in schema_names


def test_tokenise_is_idempotent_on_source_and_model() -> None:
    node = _make_node()

    first = tokenise_column(
        node, source_column="text", model="bert-base-uncased", language="en"
    )
    schema_after_first = node.data.collect_schema().names()
    derived_count_first = sum(
        name.startswith(DERIVED_PREFIX) for name in schema_after_first
    )

    second = tokenise_column(
        node, source_column="text", model="bert-base-uncased", language="en"
    )
    schema_after_second = node.data.collect_schema().names()
    derived_count_second = sum(
        name.startswith(DERIVED_PREFIX) for name in schema_after_second
    )

    # Same column name returned, no duplication.
    assert first == second
    assert derived_count_first == 1
    assert derived_count_second == 1
    assert len(node.derived) == 1


def test_tokenise_with_different_model_adds_second_column() -> None:
    node = _make_node()

    bert_name = tokenise_column(
        node, source_column="text", model="bert-base-uncased", language="en"
    )
    multi_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-multilingual-cased",
        language="en",
    )

    schema_names = node.data.collect_schema().names()
    assert bert_name in schema_names
    assert multi_name in schema_names
    assert bert_name != multi_name
    assert len(node.derived) == 2


def test_tokenise_undo_restores_prior_state() -> None:
    node = _make_node()
    schema_before = set(node.data.collect_schema().names())

    tokenise_column(
        node, source_column="text", model="bert-base-uncased", language="en"
    )
    assert node.can_undo

    node.undo()
    schema_after_undo = set(node.data.collect_schema().names())
    # LazyFrame plan is back to the original schema. The metadata index is
    # intentionally NOT rolled back here — undo restores the data plan; a
    # follow-up unregister belongs to the caller / Phase 2.5 API which
    # already wraps tokenise + DELETE in symmetric operations.
    assert schema_after_undo == schema_before


def test_tokenise_rejects_missing_source_column() -> None:
    node = _make_node()
    with pytest.raises(KeyError):
        tokenise_column(
            node, source_column="nonexistent", model="bert-base-uncased", language="en"
        )


def test_tokenise_emits_canonical_struct_dtype() -> None:
    """Phase 2.1 contract: the derived column dtype matches
    ``tokens_struct_dtype()``."""
    from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
        tokens_struct_dtype,
    )

    node = _make_node()
    derived_name = tokenise_column(
        node, source_column="text", model="bert-base-uncased", language="en"
    )
    schema = node.data.collect_schema()
    assert schema[derived_name] == tokens_struct_dtype()


def test_tokenise_chinese_via_jieba_produces_word_level_tokens() -> None:
    """Phase 1.9 + 2.3: jieba backend is reachable through tokenise_column
    and produces word-level (multi-char) Chinese segmentation."""
    df = pl.DataFrame({"text": ["今天天气很好"]}).lazy()
    node = Node(data=df, name="zh_root")

    derived_name = tokenise_column(
        node, source_column="text", model="jieba", language="zh"
    )

    collected = cast(pl.DataFrame, node.data.collect())
    tokens_lists = collected[derived_name].to_list()
    assert len(tokens_lists) == 1
    tokens = [entry["token"] for entry in tokens_lists[0]]
    # Word-level segmentation produces multi-character tokens, not pure chars.
    assert any(len(tok) > 1 for tok in tokens), (
        f"expected word-level Chinese tokens, got char-level: {tokens!r}"
    )
