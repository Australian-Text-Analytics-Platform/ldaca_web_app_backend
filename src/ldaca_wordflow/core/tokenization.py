"""Tokenization metadata operations.

Tokenisation is registered as node metadata only. Token-consuming analyses
hydrate the requested tokens from the user-specific cache into a temporary
LazyFrame when they need them; the hydrated frame is never persisted back to
``Node.data``.

Used by:
- Backend API routes, worker tasks, workspace services, and backend tests because they
  need a backend boundary that validates inputs before delegating to workspace or worker
  state.

Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
    frequencies, and persist derived artifacts for result queries.
"""

from __future__ import annotations

from typing import cast

from docworkspace import Node, TokenizationMeta

from ..api.workspaces.analyses.generated_columns import tokenization_column_name

_CASE_FREE_MODELS: frozenset[str] = frozenset(
    {
        "lindera:jieba",
        "lindera:ja-ipadic",
        "lindera:ja-unidic",
        "lindera:ko-dic",
    }
)
_REMOVE_PUNCT_DEFAULT = True


def _model_is_case_free(model: str) -> bool:
    """Support tokenization metadata helpers with a model is case free helper.

    Called by:
    - Local helpers, route handlers, or service methods in this module because they need a
      backend boundary that validates inputs before delegating to workspace or worker state.

    Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
        frequencies, and persist derived artifacts for result queries.
    """

    return model in _CASE_FREE_MODELS


def tokenise_column(
    node: Node,
    *,
    source_column: str,
    model: str,
    language: str | None,
) -> str:
    """Register a versioned tokenisation spec on ``node``.

    The function validates the source column and records enough metadata for
    analyses to hydrate tokens from the per-user DuckDB cache later. It does
    not mutate ``node.data``.

    Used by:
    - backend API routes, backend tests, core workspace and worker services because they
      need a backend boundary that validates inputs before delegating to workspace or worker
      state.

    Flow: resolve tokenization preferences, hydrate or create token columns, aggregate
        frequencies, and persist derived artifacts for result queries.
    """
    schema_names = node.data.collect_schema().names()
    if source_column not in schema_names:
        raise KeyError(
            f"Node {node.name!r} has no column {source_column!r}; "
            f"available columns: {sorted(schema_names)}"
        )

    tokenization_name = tokenization_column_name(source_column, model)

    params: dict[str, bool] = {
        "lowercase": not _model_is_case_free(model),
        "remove_punct": _REMOVE_PUNCT_DEFAULT,
    }
    tokenization_meta = cast(
        TokenizationMeta,
        {
            "column_name": tokenization_name,
            "model": model,
            "language": language,
            "params": params,
        },
    )
    node.register_tokenization(source_column, tokenization_meta)
    return tokenization_name


__all__ = ["tokenise_column"]
