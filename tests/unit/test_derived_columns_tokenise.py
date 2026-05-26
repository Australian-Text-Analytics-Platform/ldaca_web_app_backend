"""``tokenise_column`` registers cache-backed tokenization metadata."""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest
from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from ldaca_wordflow.core.derived_columns import tokenise_column
from ldaca_wordflow.core.tokens_cache import hydrate_derived_tokens_lazyframe

from docworkspace import Node


def _make_node(name: str = "root") -> Node:
    df = pl.DataFrame(
        {
            "text": ["Hello world", "Goodbye world", "Hello again"],
            "value": [1, 2, 3],
        }
    ).lazy()
    return Node(data=df, name=name)


def test_tokenise_registers_metadata_without_mutating_node_data() -> None:
    node = _make_node()
    expected_name = derived_column_name(TOKENS_FORM, "text", "bert-base-uncased")

    result_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )

    assert result_name == expected_name
    assert expected_name not in node.data.collect_schema().names()
    assert "text" in node.tokenization
    meta = node.tokenization["text"]
    assert meta["source_column"] == "text"
    assert meta["column_name"] == expected_name
    assert meta["model"] == "bert-base-uncased"
    assert meta["language"] == "en"
    assert meta["cache_backend"] == "duckdb"
    assert meta["params"] == {"lowercase": True, "remove_punct": True}
    assert meta["generated_at"]  # ISO timestamp string

    schema_names = node.data.collect_schema().names()
    assert "text" in schema_names
    assert "value" in schema_names


def test_tokenise_is_idempotent_on_source_and_model() -> None:
    node = _make_node()

    first = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    tokenization_count_first = len(node.tokenization)

    second = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    tokenization_count_second = len(node.tokenization)

    assert first == second
    assert tokenization_count_first == 1
    assert tokenization_count_second == 1
    assert len(node.tokenization) == 1


def test_tokenise_with_different_model_replaces_node_token_spec() -> None:
    node = _make_node()

    bert_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    multi_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-multilingual-cased",
        language="en",
        user_id="test_user",
    )

    assert bert_name != multi_name
    assert len(node.tokenization) == 1
    assert node.find_tokenization_column("text", model="bert-base-uncased") is None
    assert (
        node.find_tokenization_column("text", model="bert-base-multilingual-cased")
        == multi_name
    )


def test_tokenise_with_different_source_preserves_existing_token_specs() -> None:
    node = _make_node()

    text_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    value_name = tokenise_column(
        node,
        source_column="value",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )

    assert text_name != value_name
    assert len(node.tokenization) == 2
    assert node.find_tokenization_column("text") == text_name
    assert node.find_tokenization_column("value") == value_name


def test_tokenise_does_not_touch_undo_stack() -> None:
    node = _make_node()
    schema_before = set(node.data.collect_schema().names())

    tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    assert not node.can_undo
    assert set(node.data.collect_schema().names()) == schema_before


def test_tokenise_rejects_missing_source_column() -> None:
    node = _make_node()
    with pytest.raises(KeyError):
        tokenise_column(
            node,
            source_column="nonexistent",
            model="bert-base-uncased",
            language="en",
            user_id="test_user",
        )


def test_tokenise_emits_canonical_struct_dtype() -> None:
    """Hydrated tokenization columns use the canonical struct dtype."""
    from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
        tokens_struct_dtype,
    )

    node = _make_node()
    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="test_user",
    )
    hydrated = hydrate_derived_tokens_lazyframe(
        node.data,
        node=node,
        source_column="text",
        derived_name=derived_name,
        user_id="test_user",
    )
    schema = hydrated.collect_schema()
    assert schema[derived_name] == tokens_struct_dtype()


def test_tokenise_chinese_via_jieba_produces_word_level_tokens() -> None:
    """Phase 1.9 + 2.3: jieba backend is reachable through tokenise_column
    and produces word-level (multi-char) Chinese segmentation."""
    df = pl.DataFrame({"text": ["今天天气很好"]}).lazy()
    node = Node(data=df, name="zh_root")

    derived_name = tokenise_column(
        node,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="test_user",
    )

    hydrated = hydrate_derived_tokens_lazyframe(
        node.data,
        node=node,
        source_column="text",
        derived_name=derived_name,
        user_id="test_user",
    )
    collected = cast(pl.DataFrame, hydrated.collect())
    tokens_lists = collected[derived_name].to_list()
    assert len(tokens_lists) == 1
    tokens = [entry["token"] for entry in tokens_lists[0]]
    # Word-level segmentation produces multi-character tokens, not pure chars.
    assert any(len(tok) > 1 for tok in tokens), (
        f"expected word-level Chinese tokens, got char-level: {tokens!r}"
    )
