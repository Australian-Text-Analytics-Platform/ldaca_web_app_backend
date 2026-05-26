"""Derived-column metadata operations.

Tokenisation is registered as node metadata only. Token-consuming analyses
hydrate the requested tokens from the user-specific cache into a temporary
LazyFrame when they need them; the hydrated frame is never persisted back to
``Node.data``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import cast

from docworkspace import DerivedColumnMeta, Node

from ..api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)

_CASE_FREE_MODELS: frozenset[str] = frozenset(
    {
        "jieba",
        "lindera-ja-ipadic",
        "lindera-ja-unidic",
        "lindera-ko-dic",
    }
)
_REMOVE_PUNCT_DEFAULT = True


def _model_is_case_free(model: str) -> bool:
    return model in _CASE_FREE_MODELS


def _registered_token_specs(node: Node) -> list[str]:
    return [
        name
        for name, meta in node.derived.items()
        if isinstance(meta, dict) and meta.get("form") == TOKENS_FORM
    ]


def tokenise_column(
    node: Node,
    *,
    source_column: str,
    model: str,
    language: str | None,
    user_id: str,
    workspace_id: str | None = None,
) -> str:
    """Register a versioned tokenisation spec on ``node``.

    The function validates the source column and records enough metadata for
    analyses to hydrate tokens from the per-user DuckDB cache later. It does
    not mutate ``node.data``.
    """
    schema_names = node.data.collect_schema().names()
    if source_column not in schema_names:
        raise KeyError(
            f"Node {node.name!r} has no column {source_column!r}; "
            f"available columns: {sorted(schema_names)}"
        )

    derived_name = derived_column_name(TOKENS_FORM, source_column, model)
    for existing in _registered_token_specs(node):
        node.unregister_derived_column(existing)

    params: dict[str, bool] = {
        "lowercase": not _model_is_case_free(model),
        "remove_punct": _REMOVE_PUNCT_DEFAULT,
    }
    derived_meta = cast(
        DerivedColumnMeta,
        {
            "source_column": source_column,
            "form": TOKENS_FORM,
            "model": model,
            "language": language,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_backend": "duckdb",
            "params": params,
        },
    )
    node.register_derived_column(derived_name, derived_meta)
    return derived_name


__all__ = ["tokenise_column"]
