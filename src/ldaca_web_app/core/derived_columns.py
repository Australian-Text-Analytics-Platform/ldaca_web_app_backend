"""Derived-column operations (Phase 2.3 of multilingual).

Tokens and future analytic derivations live as hidden columns on the source
Node's LazyFrame (decision 7). This module owns the operation that adds or
replaces a derived tokens column and keeps ``Node.derived`` in sync.

The operation is synchronous (Polars only adds the expression to the lazy
plan; no model inference happens until a downstream tool collects the
LazyFrame). Long-running model **loads** can still be wrapped in a worker
task for progress UX, but the result mutates the source node — no child
node is created.
"""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import polars_text as pt
from docworkspace import Node

from ..api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)


def tokenise_column(
    node: Node,
    *,
    source_column: str,
    model: str,
    language: str | None,
) -> str:
    """Add or replace a derived tokens column on ``node``.

    Returns the canonical derived column name (e.g.
    ``"__derived__.tokens.text.jieba"``). Idempotent on
    ``(source_column, model)``: re-calling with the same pair replaces the
    existing column; a different model adds a second column on the same
    source. The node's LazyFrame plan is mutated in place via the ``data``
    setter, so the prior plan lands on the undo stack.

    Raises ``KeyError`` if ``source_column`` isn't present in the node's
    schema.
    """
    schema_names = node.data.collect_schema().names()
    if source_column not in schema_names:
        raise KeyError(
            f"Node {node.name!r} has no column {source_column!r}; "
            f"available columns: {sorted(schema_names)}"
        )

    derived_name = derived_column_name(TOKENS_FORM, source_column, model)
    existing = node.find_derived_column(source_column, form=TOKENS_FORM, model=model)

    tokenize_expr = pt.tokenize_with_offsets(
        pl.col(source_column), model=model
    ).alias(derived_name)

    if existing is not None:
        # Replace: drop the prior column from the plan before re-adding so
        # the resulting frame is unambiguous. strict=False guards against
        # the rare case where the column name disappeared independently
        # (e.g. external select).
        new_lf = node.data.drop(existing, strict=False).with_columns(tokenize_expr)
        node.unregister_derived_column(existing)
    else:
        new_lf = node.data.with_columns(tokenize_expr)

    node.data = new_lf

    node.register_derived_column(
        derived_name,
        {  # type: ignore[arg-type]
            "source_column": source_column,
            "form": TOKENS_FORM,
            "model": model,
            "language": language,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return derived_name


__all__ = ["tokenise_column"]
