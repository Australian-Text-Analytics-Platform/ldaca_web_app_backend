"""Frontend-facing schema projection (Phase 2.10 of multilingual).

Decision 7 keeps derived analytic specs in ``Node.derived`` under stable names
(``__derived__.<form>.<source>.<model>``). Some legacy workspaces may still
carry physical ``__derived__.*`` columns on the source LazyFrame, but cache-
backed specs are hydrated only inside analysis paths. This module owns the
single source of truth for "what does the frontend see".

Analytics tools that consume tokens look up the column via
``Node.find_derived_column(...)`` and hydrate as needed, so they are unaffected
by this filter — only user-facing surfaces are.
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

    Useful for endpoints that materialise rows for display (data view, export).
    Analytics paths that need token specs should resolve ``Node.derived`` and
    hydrate through the cache helper.
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
    if isinstance(derived, dict):
        info["derived_columns"] = sorted(derived.keys())
        # Phase 4 frontend (4.5 / 4.6 / 4.7) needs the structured metadata
        # — column name → {source_column, form, model, language,
        # generated_at} — to drive the quotation gate, inspector panel,
        # and concordance tokens-mode auto-pick. Same shape the backend
        # stores in Node.derived; ordered by column name for stable JSON.
        info["derived"] = {name: dict(derived[name]) for name in sorted(derived.keys())}
    else:
        info["derived_columns"] = []
        info["derived"] = {}
    return info


__all__ = ["visible_column_names", "project_visible", "frontend_node_info"]
