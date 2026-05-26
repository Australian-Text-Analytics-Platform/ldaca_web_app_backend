"""Frontend-facing schema projection.

Token outputs are cache-backed and hydrated only inside analysis paths; node
schemas no longer need special hidden-column filtering. This module preserves
the physical schema while surfacing ``Node.derived`` metadata for UI affordances
that need to know whether a node has been tokenised.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

import polars as pl

from docworkspace import Node


def visible_column_names(columns: Iterable[str]) -> list[str]:
    """Return physical column names unchanged, preserving original order."""
    return list(columns)


def project_visible(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Return a LazyFrame projected onto the user-facing columns only.

    Useful for endpoints that materialise rows for display (data view, export).
    Analytics paths that need token specs should resolve ``Node.derived`` and
    hydrate through the cache helper.
    """
    visible = visible_column_names(lf.collect_schema().names())
    return lf.select(visible)


def frontend_node_info(node: Node) -> dict[str, Any]:
    """Return :meth:`Node.info` with structured token metadata attached."""
    info = node.info()
    columns: Sequence[str] = info.get("columns", [])
    schema: dict[str, Any] = info.get("schema", {})
    filtered_columns = visible_column_names(columns)
    filtered_schema = dict(schema)

    height, _full_width = info.get("shape", (0, len(columns)))
    info["columns"] = filtered_columns
    info["schema"] = filtered_schema
    info["shape"] = (height, len(filtered_columns))
    # Guard against test mocks where ``node.derived`` isn't a real dict.
    derived = getattr(node, "derived", None)
    if isinstance(derived, dict):
        info["derived"] = {name: dict(derived[name]) for name in sorted(derived.keys())}
    else:
        info["derived"] = {}
    return info


__all__ = ["visible_column_names", "project_visible", "frontend_node_info"]
