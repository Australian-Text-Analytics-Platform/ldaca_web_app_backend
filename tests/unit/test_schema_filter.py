"""Frontend-facing schema projection for token metadata.

Token columns are dynamically hydrated for analyses and are not expected to
live in ``node.data``. Schema projection therefore preserves the physical
LazyFrame schema and only surfaces lightweight token metadata separately.
"""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest
from ldaca_wordflow.api.workspaces.schema_filter import (
    frontend_node_info,
    project_visible,
    visible_column_names,
)

from docworkspace import DerivedColumnMeta, Node

_DERIVED_NAME = "text.tokenization.jieba"


def _meta(source: str = "text", model: str = "jieba") -> DerivedColumnMeta:
    return {
        "source_column": source,
        "form": "tokens",
        "model": model,
        "language": "zh",
        "generated_at": "2026-05-12T00:00:00+00:00",
    }


def _make_node_with_derived() -> Node:
    df = pl.DataFrame(
        {
            "text": ["a", "b"],
            "value": [1, 2],
            _DERIVED_NAME: [
                [{"token": "a", "start": 0, "end": 1}],
                [{"token": "b", "start": 0, "end": 1}],
            ],
        }
    ).lazy()
    node = Node(data=df, name="root")
    node.register_derived_column(_DERIVED_NAME, _meta())
    return node


def test_visible_column_names_preserves_physical_columns() -> None:
    columns = ["text", "value", _DERIVED_NAME]
    assert visible_column_names(columns) == columns


def test_project_visible_preserves_lazyframe_columns() -> None:
    node = _make_node_with_derived()

    projected = project_visible(node.data)
    projected_columns = projected.collect_schema().names()
    assert _DERIVED_NAME in projected_columns
    assert "text" in projected_columns
    assert "value" in projected_columns


def test_frontend_node_info_preserves_schema_and_reports_token_metadata() -> None:
    node = _make_node_with_derived()
    info = frontend_node_info(node)

    assert _DERIVED_NAME in info["columns"]
    assert _DERIVED_NAME in info["schema"]
    assert info["columns"] == ["text", "value", _DERIVED_NAME]
    assert info["shape"][1] == 3
    assert info["derived"] == {_DERIVED_NAME: _meta()}


def test_frontend_node_info_round_trips_empty_derived() -> None:
    df = pl.DataFrame({"text": ["a"]}).lazy()
    node = Node(data=df, name="plain")
    info = frontend_node_info(node)
    assert info["columns"] == ["text"]
    assert info["derived"] == {}
    assert info["shape"][1] == 1


def test_project_visible_preserves_row_order_and_counts() -> None:
    """The filter is column-projection only — row order/count unchanged."""
    df = pl.DataFrame(
        {
            "text": ["a", "b", "c"],
            _DERIVED_NAME: [
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
