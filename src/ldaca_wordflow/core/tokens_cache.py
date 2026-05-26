"""Wordflow integration for the polars-text token cache.

Nodes store per-column versioned tokenisation specs in ``Node.tokenization``. Analyses
attach those specs to a LazyFrame with ``hydrate_tokenization_lazyframe``.
The generic DuckDB cache mechanics live in ``pl.col(...).text.tokenize(...,
cache=...)``; this module owns only Wordflow's per-user cache path and node
metadata hydration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import polars as pl
import polars_text  # noqa: F401

from .utils import get_user_cache_folder

TOKENS_CACHE_FILENAME = "tokens.duckdb"


def tokens_cache_path(user_id: str) -> Path:
    """Return the per-user DuckDB token cache path."""
    return get_user_cache_folder(user_id) / TOKENS_CACHE_FILENAME


def hydrate_tokenization_lazyframe(
    base_lf: pl.LazyFrame,
    *,
    node: Any,
    source_column: str,
    tokenization_column: str,
    user_id: str,
) -> pl.LazyFrame:
    """Lazily attach a tokenization column registered on ``node``.

    Short-circuits if the column is already physically present. Otherwise
    reads the model and tokenisation params from
    ``node.tokenization[source_column]``
    and attaches a cache-backed elementwise expression keyed on ``user_id``.
    """
    if tokenization_column in base_lf.collect_schema().names():
        return base_lf

    tokenization_registry = getattr(node, "tokenization", {})
    tokenization_meta = (
        tokenization_registry.get(source_column)
        if isinstance(tokenization_registry, dict)
        else None
    )
    if not isinstance(tokenization_meta, dict):
        return base_lf
    if tokenization_meta.get("column_name") != tokenization_column:
        return base_lf

    model = tokenization_meta.get("model")
    params = tokenization_meta.get("params") or {}
    if not isinstance(model, str):
        return base_lf

    return base_lf.with_columns(
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
