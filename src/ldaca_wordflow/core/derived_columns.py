"""Derived-column operations — lazy on-demand tokenisation.

Tokens and future analytic derivations live as hidden columns on the source
Node's LazyFrame (decision 7). This module owns the operation that adds or
replaces a derived tokens column and keeps ``Node.derived`` in sync.

The node's data plan carries a ``polars_text.tokenize_with_cache_lookup``
expression that resolves its cache directory from
``LDACA_TOKENS_CACHE_DIR + user_id`` at execution time. The serialised
plan therefore never bakes absolute paths, so workspace ``.plbin`` files
are cross-machine portable by construction — see
``docs/developer-guide/lazy-tokenisation-refactor.md``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from docworkspace import Node

from ..api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from . import tokens_cache

logger = logging.getLogger(__name__)


# Models whose backend scripts have no case (Chinese, Japanese, Korean).
# Passing ``lowercase=True`` to these would burn a full Unicode case-fold
# walk plus a heap allocation per row for zero semantic effect — minutes
# of CPU on a CJK corpus. The Rust ``preprocess`` helper short-circuits
# defensively, but being explicit here makes the intent obvious at the
# call site and survives a future refactor that drops the safety net.
_CASE_FREE_MODELS: frozenset[str] = frozenset(
    {
        "jieba",
        "lindera-ja-ipadic",
        "lindera-ja-unidic",
        "lindera-ko-dic",
    }
)

# ``polars_text.tokenize_with_cache_lookup`` defaults to ``remove_punct=True``.
# Kept here so the cache key (which folds params into a hash) and the
# expression's kwargs always agree.
_REMOVE_PUNCT_DEFAULT = True


def _model_is_case_free(model: str) -> bool:
    return model in _CASE_FREE_MODELS


def tokenise_column(
    node: Node,
    *,
    source_column: str,
    model: str,
    language: Optional[str],
    user_id: str,
    workspace_id: Optional[str] = None,
) -> str:
    """Add or replace a derived tokens column on ``node``.

    Returns the canonical derived column name (e.g.
    ``"__derived__.tokens.text.jieba"``). Idempotent on
    ``(source_column, model)``: re-calling with the same pair replaces the
    existing column; a different model adds a second column on the same
    source.

    The call returns immediately — no tokens are materialised. The plan
    is stamped with a ``polars_text.tokenize_with_cache_lookup``
    expression that fills the cache on the first analysis collect.

    The cache is per-user (resolved from
    ``LDACA_TOKENS_CACHE_DIR/{user_id}/tokens/``), so ``user_id`` is
    required. ``workspace_id`` is optional — when supplied, a manifest
    reference is registered so the sweep can reclaim the cache when no
    node is using it.

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
    existing = node.find_derived_column(
        source_column, form=TOKENS_FORM, model=model
    )

    lowercase = not _model_is_case_free(model)
    params = {"lowercase": lowercase, "remove_punct": _REMOVE_PUNCT_DEFAULT}

    # Strip any previously-derived tokens column from the plan before we
    # rebuild it. Using the un-derived base as the source for hashing is
    # important — if a prior derivation is already present, we want to
    # hash the original ``source_column``, not the post-derivation frame.
    if existing is not None:
        base_lf = node.data.drop(existing, strict=False)
    else:
        base_lf = node.data

    cache_filename = tokens_cache.cache_filename(model, params)
    cache_path = tokens_cache.cache_path(user_id, model, params)
    new_lf = _build_lazy_plan(
        base_lf=base_lf,
        source_column=source_column,
        user_id=user_id,
        model=model,
        params=params,
        bucket_filename=cache_filename,
        derived_name=derived_name,
    )

    if existing is not None:
        node.unregister_derived_column(existing)
    node.data = new_lf

    node.register_derived_column(
        derived_name,
        {  # type: ignore[arg-type]
            "source_column": source_column,
            "form": TOKENS_FORM,
            "model": model,
            "language": language,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_filename": cache_path.name,
        },
    )

    # Reference the cache so the sweep keeps it alive while this node is
    # using it. ``workspace_id`` is optional in unit-test contexts that
    # don't wire up a workspace — skipping the reference there means the
    # test cache file looks orphaned to the sweep, which is bounded by
    # the tmpdir the autouse fixture isolates.
    if workspace_id:
        tokens_cache.add_reference(
            user_id,
            cache_path.name,
            tokens_cache.CacheReference(
                workspace_id=workspace_id,
                node_id=str(getattr(node, "id", node.name)),
            ),
        )

    # Each cache miss inside `tokenize_with_cache_lookup` writes a fresh
    # `<bucket>__delta__<uuid>.parquet`, so a bucket that the user
    # tokenises against many slightly-different source corpora accumulates
    # one delta per analysis run. Compact opportunistically — cheap when
    # below threshold (just a directory listing), and capped per call so
    # we never block tokenise_column waiting on a giant merge. Best-effort:
    # a failure here doesn't affect the lazy plan we just stamped.
    try:
        tokens_cache.compact_bucket_if_needed(user_id, model, params)
    except Exception:  # pragma: no cover — defensive
        logger.exception(
            "compact_bucket_if_needed failed for user=%s model=%s; ignoring",
            user_id,
            model,
        )

    return derived_name


def _build_lazy_plan(
    *,
    base_lf,
    source_column: str,
    user_id: str,
    model: str,
    params: dict,
    bucket_filename: str,
    derived_name: str,
):
    """Wrap ``base_lf`` with the lazy ``tokenize_with_cache_lookup``
    expression. No eager work — the Rust expression reads (or creates)
    the bucket dir at execution time and writes back any cache-miss
    tokens as a fresh ``<bucket>__delta__<uuid>.parquet``.
    """
    import polars as pl
    import polars_text as pt

    return base_lf.with_columns(
        pt.tokenize_with_cache_lookup(
            pl.col(source_column),
            user_id=user_id,
            bucket_filename=bucket_filename,
            lowercase=params["lowercase"],
            remove_punct=params["remove_punct"],
            model=model,
            # False so test environments (which set the env via conftest)
            # work without further plumbing; production wires the env at
            # backend startup so all collects find the right per-user dir.
            require_env_cache_dir=False,
        ).alias(derived_name)
    )


__all__ = ["tokenise_column"]
