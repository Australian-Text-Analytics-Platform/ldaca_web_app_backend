"""Phase 2.10: derived columns are hidden from frontend-facing schema
projections. ``__derived__.*`` lives in ``node.data`` for analytics tools
to consume but never appears in node info, data-view payloads, or export
schemas.
"""

from __future__ import annotations

from typing import cast

import polars as pl
import pytest
from docworkspace import Node

from ldaca_web_app.api.workspaces.schema_filter import (
    frontend_node_info,
    project_visible,
    visible_column_names,
)


_DERIVED_NAME = "__derived__.tokens.text.jieba"


def _meta(source: str = "text", model: str = "jieba") -> dict:
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
    node.register_derived_column(_DERIVED_NAME, _meta())  # type: ignore[arg-type]
    return node


def test_visible_column_names_strips_derived_prefix() -> None:
    columns = ["text", "value", _DERIVED_NAME, "__derived__.pos.text.spacy-en"]
    assert visible_column_names(columns) == ["text", "value"]


def test_project_visible_drops_derived_columns_from_lazyframe() -> None:
    node = _make_node_with_derived()

    projected = project_visible(node.data)
    projected_columns = projected.collect_schema().names()
    assert _DERIVED_NAME not in projected_columns
    assert "text" in projected_columns
    assert "value" in projected_columns

    # node.data is unchanged (the analytics path keeps full schema).
    assert _DERIVED_NAME in node.data.collect_schema().names()


def test_frontend_node_info_hides_derived_from_schema_and_columns() -> None:
    node = _make_node_with_derived()
    info = frontend_node_info(node)

    assert _DERIVED_NAME not in info["columns"]
    assert _DERIVED_NAME not in info["schema"]
    assert info["columns"] == ["text", "value"]
    # Shape width is adjusted to match the visible column count.
    assert info["shape"][1] == 2
    # Derived columns surface separately so the frontend can list them.
    assert info["derived_columns"] == [_DERIVED_NAME]
    # Phase 4: structured metadata travels alongside the name list so the
    # frontend can drive the quotation gate / concordance auto-pick /
    # inspector panel without having to fetch the workspace state again.
    assert info["derived"] == {_DERIVED_NAME: _meta()}


def test_frontend_node_info_round_trips_empty_derived() -> None:
    df = pl.DataFrame({"text": ["a"]}).lazy()
    node = Node(data=df, name="plain")
    info = frontend_node_info(node)
    assert info["columns"] == ["text"]
    assert info["derived_columns"] == []
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
