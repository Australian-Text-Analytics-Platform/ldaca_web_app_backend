"""Frontend-facing schema projection (Phase 2.10 of pluggable_tokeniser).

Decision 7 keeps derived analytic columns on the source node's LazyFrame
(``__derived__.<form>.<source>.<model>``), but the user shouldn't see them
in the data view, node info panel, or export schema. This module owns the
single source of truth for "what does the frontend see".

Analytics tools that consume tokens look up the column via
``Node.find_derived_column(...)`` against the FULL ``node.data`` schema, so
they are unaffected by this filter — only user-facing surfaces are.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

import polars as pl
from docworkspace import Node

from .analyses.generated_columns import is_derived_column_name


def visible_column_names(columns: Iterable[str]) -> list[str]:
    """Drop ``__derived__.*`` entries while preserving original order."""
    return [name for name in columns if not is_derived_column_name(name)]


def project_visible(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Return a LazyFrame projected onto the user-facing columns only.

    Useful for endpoints that materialise rows for display (data view,
    export). Analytics paths that need the derived columns should consume
    ``node.data`` directly.
    """
    visible = visible_column_names(lf.collect_schema().names())
    return lf.select(visible)


def frontend_node_info(node: Node) -> dict[str, Any]:
    """Like :meth:`Node.info` but with derived columns hidden.

    ``columns`` and ``schema`` are filtered; ``shape`` width is adjusted to
    match. The full set of registered derived column names is surfaced
    separately as ``derived_columns`` so a frontend panel can list them
    explicitly without leaking them into the main schema view.
    """
    info = node.info()
    columns: Sequence[str] = info.get("columns", [])
    schema: dict[str, Any] = info.get("schema", {})
    filtered_columns = visible_column_names(columns)
    filtered_schema = {
        col: dtype for col, dtype in schema.items() if not is_derived_column_name(col)
    }

    height, _full_width = info.get("shape", (0, len(columns)))
    info["columns"] = filtered_columns
    info["schema"] = filtered_schema
    info["shape"] = (height, len(filtered_columns))
    # Guard against test mocks where ``node.derived`` isn't a real dict.
    derived = getattr(node, "derived", None)
    info["derived_columns"] = sorted(derived.keys()) if isinstance(derived, dict) else []
    return info


__all__ = ["visible_column_names", "project_visible", "frontend_node_info"]
