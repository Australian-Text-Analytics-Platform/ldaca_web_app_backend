"""Derived-column operations (Phase 2.3 of multilingual).

Tokens and future analytic derivations live as hidden columns on the source
Node's LazyFrame (decision 7). This module owns the operation that adds or
replaces a derived tokens column and keeps ``Node.derived`` in sync.

Phase 5 (perf): the tokens column is now backed by a persistent on-disk
cache (``tokens_cache``). ``tokenise_column`` eagerly materialises tokens
for the new content into a per-``(model, params)`` parquet, then rewrites
``node.data`` to *join* the cache by content hash instead of carrying the
tokeniser expression in the lazy plan. Result: every downstream collect
(concordance page, token-freq run, page-size probe) reads cached tokens
instead of re-running the tokeniser — the structural fix for the CJK
perf regression. Child blocks derived from this node inherit the join
plan and share the same cache rows via hash matching.

The call still blocks until tokenisation finishes (it always did — the
prior version only deferred the cost to the next collect). A future
revision can wrap the upsert step in a worker task for a progress UX.
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

# ``polars_text.tokenize_with_offsets`` defaults to ``remove_punct=True``.
# We don't expose an override yet — keep the value in one place so the
# cache key and the upsert call always agree.
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

    The node's LazyFrame plan is mutated in place via the ``data`` setter
    (so the prior plan lands on the undo stack), but the new plan now
    *joins* the tokens cache parquet by content hash instead of carrying
    the tokenise expression — the perf fix for repeated collects.

    The cache is per-user (lives at ``{user_root}/user_cache/tokens/``),
    so ``user_id`` is required for path resolution. ``workspace_id`` is
    optional — when supplied, a manifest reference is registered so the
    sweep can reclaim the parquet when no node is using it; when
    ``None`` (e.g. unit tests that don't wire up a workspace) the
    upsert still happens but no reference is recorded.

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
    # rebuild it, so the replacement is unambiguous. Use the un-derived
    # base plan as the source for hashing — if a prior cache-join is
    # already present, we want to hash the original ``source_column``,
    # not the post-join wider frame.
    if existing is not None:
        base_lf = node.data.drop(existing, strict=False)
    else:
        base_lf = node.data

    # Upsert: tokenise any rows whose content hash isn't in the cache,
    # then return the canonical bucket path (used purely as a stable
    # identifier in derived metadata — the actual files for this bucket
    # live alongside it as ``<bucket>__delta__*.parquet`` siblings).
    cache_path = _upsert_for_node(
        base_lf=base_lf,
        source_column=source_column,
        model=model,
        params=params,
        user_id=user_id,
    )

    # Rewrite ``node.data`` to look up the tokens column by content-hash
    # join over the union of every delta file in this bucket.
    new_lf = _build_cache_join(
        base_lf=base_lf,
        source_column=source_column,
        user_id=user_id,
        model=model,
        params=params,
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

    # Reference the cache so the sweep keeps it alive while this node
    # is using it. ``workspace_id`` is optional in unit-test contexts
    # that don't wire up a workspace — skipping the reference there
    # means the test cache file looks orphaned to the sweep, which is
    # bounded by the tmpdir the autouse fixture isolates.
    if workspace_id:
        tokens_cache.add_reference(
            user_id,
            cache_path.name,
            tokens_cache.CacheReference(
                workspace_id=workspace_id,
                node_id=str(getattr(node, "id", node.name)),
            ),
        )

    return derived_name


def _upsert_for_node(
    *,
    base_lf,
    source_column: str,
    model: str,
    params: dict,
    user_id: str,
):
    """Materialise tokens for the node's source column into the cache.

    Reads existing cache content hashes for this user, filters to the
    rows that aren't cached yet, tokenises those, and appends the
    result. Returns the cache parquet path.
    """
    import polars as pl
    import polars_text as pt

    cached_hashes = tokens_cache.read_cached_hashes(user_id, model, params)

    # Select + hash + dedupe in one lazy plan so the source is only
    # read once. ``unique`` collapses duplicate documents within the
    # source so identical strings tokenise exactly once.
    new_rows_lf = (
        base_lf.select(
            pl.col(source_column).hash().alias(tokens_cache.CONTENT_HASH_COLUMN),
            pl.col(source_column).alias("__src__"),
        )
        .unique(subset=[tokens_cache.CONTENT_HASH_COLUMN])
    )
    if cached_hashes:
        new_rows_lf = new_rows_lf.filter(
            ~pl.col(tokens_cache.CONTENT_HASH_COLUMN).is_in(list(cached_hashes))
        )

    new_tokens_df = new_rows_lf.select(
        pl.col(tokens_cache.CONTENT_HASH_COLUMN),
        pt.tokenize_with_offsets(
            pl.col("__src__"),
            model=model,
            lowercase=params["lowercase"],
            remove_punct=params["remove_punct"],
        ).alias("tokens"),
    ).collect()

    if new_tokens_df.height > 0:
        return tokens_cache.write_or_append_cache(
            user_id, model, params, new_tokens_df
        )

    # Nothing new to write. Normally the bucket already has files from
    # the cache hit and we can return the canonical bucket path. The
    # rare edge case where every file vanished between the hash read
    # and now (a parallel sweep, manual cleanup) is handled by
    # re-running the upsert with all rows. ``cache_exists`` checks for
    # *any* bucket file (legacy or delta), not just the canonical
    # ``<bucket>.parquet`` (which post-refactor is rarely on disk).
    if not tokens_cache.cache_exists(user_id, model, params):
        # Race recovery: rebuild with all rows.
        all_rows_lf = base_lf.select(
            pl.col(source_column).hash().alias(tokens_cache.CONTENT_HASH_COLUMN),
            pl.col(source_column).alias("__src__"),
        ).unique(subset=[tokens_cache.CONTENT_HASH_COLUMN])
        all_tokens_df = all_rows_lf.select(
            pl.col(tokens_cache.CONTENT_HASH_COLUMN),
            pt.tokenize_with_offsets(
                pl.col("__src__"),
                model=model,
                lowercase=params["lowercase"],
                remove_punct=params["remove_punct"],
            ).alias("tokens"),
        ).collect()
        return tokens_cache.write_or_append_cache(
            user_id, model, params, all_tokens_df
        )
    return tokens_cache.cache_path(user_id, model, params)


def _build_cache_join(
    *,
    base_lf,
    source_column: str,
    user_id: str,
    model: str,
    params: dict,
    derived_name: str,
):
    """Wrap ``base_lf`` to attach the cached tokens by content hash.

    The added plan is intentionally simple so the optimiser can push
    slices and predicates through it: hash → left join → drop the
    helper hash column. The result frame has every original column plus
    one new ``derived_name`` list column.

    Reads the bucket as a deduplicated union of every file owned by
    this (user, model, params) tuple — see
    :func:`tokens_cache.tokens_cache_lazyframe`. The lazy plan baked
    into ``base_lf`` is fresh at every ``tokenise_column`` call, so it
    always reflects the current set of delta files when the user
    re-tokenises.
    """
    import polars as pl

    cache_lf = tokens_cache.tokens_cache_lazyframe(user_id, model, params)
    if cache_lf is None:
        # Should not happen — ``_upsert_for_node`` guarantees at least one
        # file exists in the bucket before this is called. Raise rather
        # than silently masking the bug by joining against an empty frame.
        raise RuntimeError(
            f"tokens cache bucket is empty for user={user_id!r} model={model!r}; "
            "_upsert_for_node should have populated it"
        )
    cache_lf = cache_lf.rename({"tokens": derived_name})
    return (
        base_lf.with_columns(
            pl.col(source_column).hash().alias(tokens_cache.CONTENT_HASH_COLUMN)
        )
        .join(cache_lf, on=tokens_cache.CONTENT_HASH_COLUMN, how="left")
        .drop(tokens_cache.CONTENT_HASH_COLUMN)
    )


__all__ = ["tokenise_column"]
