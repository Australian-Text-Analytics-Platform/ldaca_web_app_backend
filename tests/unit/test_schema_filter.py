"""Frontend-facing schema projection for token metadata.

Token columns are dynamically hydrated for analyses and are not expected to
live in ``node.data``. Schema projection therefore preserves the physical
LazyFrame schema and only surfaces lightweight token metadata separately.
"""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest
from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    tokenization_column_name,
)
from ldaca_wordflow.api.workspaces.schema_filter import (
    frontend_node_info,
    project_visible,
    visible_column_names,
)

from docworkspace import Node, TokenizationMeta

_TOKENS_NAME = "tokenization.text.lindera:jieba"


def _meta(source: str = "text", model: str = "lindera:jieba") -> TokenizationMeta:
    return {
        "column_name": tokenization_column_name(source, model),
        "model": model,
        "language": "zh",
        "params": {"lowercase": True, "remove_punct": True},
    }


def _make_node_with_tokenization() -> Node:
    df = pl.DataFrame(
        {
            "text": ["a", "b"],
            "value": [1, 2],
            _TOKENS_NAME: [
                [{"token": "a", "start": 0, "end": 1}],
                [{"token": "b", "start": 0, "end": 1}],
            ],
        }
    ).lazy()
    node = Node(data=df, name="root")
    node.register_tokenization("text", _meta())
    return node


def test_visible_column_names_preserves_physical_columns() -> None:
    columns = ["text", "value", _TOKENS_NAME]
    assert visible_column_names(columns) == columns


def test_project_visible_preserves_lazyframe_columns() -> None:
    node = _make_node_with_tokenization()

    projected = project_visible(node.data)
    projected_columns = projected.collect_schema().names()
    assert _TOKENS_NAME in projected_columns
    assert "text" in projected_columns
    assert "value" in projected_columns


def test_frontend_node_info_preserves_schema() -> None:
    node = _make_node_with_tokenization()
    info = frontend_node_info(node)

    assert _TOKENS_NAME in info["columns"]
    assert _TOKENS_NAME in info["schema"]
    assert info["columns"] == ["text", "value", _TOKENS_NAME]
    assert info["shape"][1] == 3
    assert "tokenization" not in info


def test_frontend_node_info_plain_node() -> None:
    df = pl.DataFrame({"text": ["a"]}).lazy()
    node = Node(data=df, name="plain")
    info = frontend_node_info(node)
    assert info["columns"] == ["text"]
    assert "tokenization" not in info
    assert info["shape"][1] == 1


def test_project_visible_preserves_row_order_and_counts() -> None:
    """The filter is column-projection only — row order/count unchanged."""
    df = pl.DataFrame(
        {
            "text": ["a", "b", "c"],
            _TOKENS_NAME: [
                [{"token": "a", "start": 0, "end": 1}],
                [{"token": "b", "start": 0, "end": 1}],
                [{"token": "c", "start": 0, "end": 1}],
            ],
        }
    ).lazy()
    projected = project_visible(df)
    collected = cast(pl.DataFrame, projected.collect())
    assert collected.height == 3
    assert collected["text"].to_list() == ["a", "b", "c"]
