"""Frontend-facing schema projection.

Token outputs are cache-backed and hydrated only inside analysis paths; node
schemas no longer need special hidden-column filtering. This module preserves
the physical schema for UI consumption.

Used by:
- FastAPI workspace routers, frontend workspace features, and backend tests because they need this unit's "Frontend-facing schema projection" behavior.

Flow:
- Workspace routes pass node schemas and LazyFrames through these helpers before returning UI payloads.
- Helpers preserve physical columns while keeping projection behavior centralized for future filtering.
- Responses keep `Node.info()` shape/schema values consistent with frontend-visible columns.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

import polars as pl

from docworkspace import Node


def visible_column_names(columns: Iterable[str]) -> list[str]:
    """Return physical column names unchanged, preserving original order.

    Used by:
    - backend API routes, backend tests because they need this unit's "Return physical column names unchanged, preserving original order" behavior.
    """
    return list(columns)


def project_visible(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Return a LazyFrame projected onto the user-facing columns only.

    Useful for endpoints that materialise rows for display (data view, export).
    Analytics paths that need token specs should resolve ``Node.tokenization`` and
    hydrate through the cache helper.

    Used by:
    - backend API routes, backend tests because they need this unit's "Return a LazyFrame projected onto the user-facing columns only" behavior.
    """
    visible = visible_column_names(lf.collect_schema().names())
    return lf.select(visible)


def frontend_node_info(node: Node) -> dict[str, Any]:
    """Return :meth:`Node.info` projected for frontend consumption.

    Steps:
    - Normalize caller input into the representation this module expects.
    - Delegate stateful, expensive, or validating work to the owning manager/helper when needed.
    - Return the compact value the caller uses for artifacts, validation, or response shaping.

    Used by:
    - backend API routes, backend tests because they need this unit's "Return :meth:`Node.info` projected for frontend consumption" behavior.
    """
    info = node.info()
    if "id" not in info and "node_id" in info:
        info["id"] = info["node_id"]
    columns: Sequence[str] = info.get("columns", [])
    schema: dict[str, Any] = info.get("schema", {})
    filtered_columns = visible_column_names(columns)
    filtered_schema = dict(schema)

    height, _full_width = info.get("shape", (0, len(columns)))
    info["columns"] = filtered_columns
    info["schema"] = filtered_schema
    info["shape"] = (height, len(filtered_columns))
    return info


__all__ = ["visible_column_names", "project_visible", "frontend_node_info"]
