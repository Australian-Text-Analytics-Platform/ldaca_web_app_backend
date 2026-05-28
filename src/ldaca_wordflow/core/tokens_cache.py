"""Wordflow integration for the polars-text token cache.

Nodes store per-column tokenisation specs in ``Node.tokenization``. Analyses
attach those specs to a LazyFrame with ``hydrate_tokenization_lazyframe``.
The generic DuckDB cache mechanics live in ``pl.col(...).text.tokenize(...,
cache=...)``; this module owns only Wordflow's per-user cache path and node
metadata hydration.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
    frequencies, and persist derived artifacts for result queries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import polars as pl
import polars_text  # noqa: F401

from .utils import get_user_cache_folder

TOKENS_CACHE_FILENAME = "tokens.duckdb"


def tokens_cache_path(user_id: str) -> Path:
    """Return the per-user DuckDB token cache path.

    Used by:
    - backend tests, core workspace and worker services because tests need the same
      observable contract that production routes and workers rely on.

    Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
        frequencies, and persist derived artifacts for result queries.
    """
    return get_user_cache_folder(user_id) / TOKENS_CACHE_FILENAME


def hydrate_tokenization_lazyframe(
    *,
    node: Any,
    source_column: str,
    user_id: str,
) -> pl.LazyFrame:
    """Lazily attach a tokenization column registered on ``node``.

    Short-circuits if the column is already physically present. Otherwise reads
    the model, token column, and tokenisation params from
    ``node.tokenization[source_column]`` and attaches a cache-backed elementwise
    expression keyed on ``user_id``.

    Used by:
    - backend API routes, backend tests, core workspace and worker services because they
      need a backend boundary that validates inputs before delegating to workspace or worker
      state.

    Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
        frequencies, and persist derived artifacts for result queries.
    """
    tokenization_registry = getattr(node, "tokenization", {})
    tokenization_meta = (
        tokenization_registry.get(source_column)
        if isinstance(tokenization_registry, dict)
        else None
    )
    if not isinstance(tokenization_meta, dict):
        return node.data

    tokenization_column = tokenization_meta.get("column_name")
    model = tokenization_meta.get("model")
    params = tokenization_meta.get("params") or {}
    if not isinstance(tokenization_column, str) or not isinstance(model, str):
        return node.data

    if tokenization_column in node.data.collect_schema().names():
        return node.data

    return node.data.with_columns(
        cast(Any, pl.col(source_column))
        .text.tokenize(
            lowercase=bool(params.get("lowercase", True)),
            remove_punct=bool(params.get("remove_punct", True)),
            model=model,
            cache=tokens_cache_path(user_id),
        )
        .alias(tokenization_column)
    )


__all__ = [
    "TOKENS_CACHE_FILENAME",
    "hydrate_tokenization_lazyframe",
    "tokens_cache_path",
]
