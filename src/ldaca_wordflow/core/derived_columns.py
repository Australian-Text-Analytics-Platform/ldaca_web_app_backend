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
from polars_text import scrub_plugin_expressions

from ..api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from . import tokens_cache


# Identifier of the polars-text FFI plugin function we stamp into the
# lazy plan in :func:`_build_lazy_plan`. Used by
# :func:`scrub_plugin_expressions` to disambiguate the targeted
# expression from any user-built expression that may share an alias.
_TOKENS_PLUGIN_SYMBOL = "tokenize_with_cache_lookup"

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
    # rebuild it. We scrub the underlying ``tokenize_with_cache_lookup``
    # expression (not just drop the alias) so the saved plan stays
    # minimal: polars' optimiser would DCE the dropped expression at
    # collect time, but the DSL itself would still grow by one HStack
    # per re-tokenisation, bloating ``.plbin`` files across many shares
    # / re-tokenisations. See ``polars_text.scrub_plugin_expressions``.
    if existing is not None:
        base_lf, _ = scrub_plugin_expressions(
            node.data,
            aliases=[existing],
            symbol=_TOKENS_PLUGIN_SYMBOL,
        )
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

    # ``cache_relpath`` is the path the lazy expression bakes into its
    # ``user_id`` kwarg (the per-user cache subdir relative to
    # ``LDACA_TOKENS_CACHE_DIR``). Storing it on the derived metadata
    # is how the workspace-load alignment hook detects a cross-user
    # import: stored != current means the plan was authored by someone
    # else and must be scrubbed-and-recreated for the current user
    # before any analysis writes a cache parquet. See
    # :func:`align_tokens_for_current_user`.
    node.register_derived_column(
        derived_name,
        {  # type: ignore[arg-type]
            "source_column": source_column,
            "form": TOKENS_FORM,
            "model": model,
            "language": language,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "cache_filename": cache_path.name,
            "cache_relpath": tokens_cache.cache_relpath_for_user(user_id),
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

    The ``user_id`` kwarg passed to the Rust expression is the
    Wordflow-convention cache-path-relative-to-env (e.g.
    ``user_root/user_cache`` in single-user mode), NOT the bare auth
    user id. This lands the cache file inside the per-user data tree
    (sibling of ``embeddings/`` under ``user_cache/``) so backups, ACLs,
    and per-user cleanup all see the cache as part of the user's
    own data.

    Workspace load detects mismatches between this baked path and the
    currently authenticated user's expected path and scrubs+recreates
    the expression so a workspace imported by user B never writes into
    user A's tree — the kwarg is an ephemeral runtime binding to the
    CURRENTLY authenticated user.
    """
    import polars as pl
    import polars_text as pt

    return base_lf.with_columns(
        pt.tokenize_with_cache_lookup(
            pl.col(source_column),
            user_id=tokens_cache.cache_relpath_for_user(user_id),
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


def align_tokens_for_current_user(workspace, user_id: str) -> int:
    """Detect cross-user-imported tokenised columns and realign their
    cache path with ``user_id``'s own tree.

    Called from `WorkspaceManager.set_current_workspace` after
    `Workspace.load()`. For each node with a tokens-form derived
    column, compare the stored ``cache_relpath`` (the path baked into
    the lazy expression's ``user_id`` kwarg at tokenise time) with the
    path the *current* user resolves to. On a mismatch, scrub the old
    `tokenize_with_cache_lookup` expression from the plan and call
    :func:`tokenise_column` again to stamp a fresh one under the
    current user's identity.

    This is the multi-user-safety contract for shared workspaces. The
    cache content is content-addressed (hash of source text + model
    params) so the realignment is semantically safe — a content hit
    in the original author's tree would have been a content hit in
    the importer's tree too. We simply ensure the new writes land
    where the new collects look.

    Path comparison (rather than user-id comparison) is intentional.
    Two users may share a literal id across Linux and Windows hosts
    while their per-OS path resolution differs; comparing the
    resolved relpath catches all such cases. It also catches the
    single-user → multi-user migration case, where the same physical
    user moves from ``user_root/user_cache`` to ``user_<id>/user_cache``.

    Returns the number of derived columns realigned. Best-effort: a
    failure on one node is logged and swallowed so a single bad node
    can't block opening the rest of the workspace.
    """
    nodes_attr = getattr(workspace, "nodes", None)
    if nodes_attr is None:
        return 0
    nodes = nodes_attr.values() if hasattr(nodes_attr, "values") else nodes_attr

    workspace_id = getattr(workspace, "id", None) or getattr(
        workspace, "workspace_id", None
    )
    current_relpath = tokens_cache.cache_relpath_for_user(user_id)

    realigned = 0
    for node in nodes:
        derived = getattr(node, "derived", None) or {}
        # Snapshot — ``tokenise_column`` mutates ``node.derived`` via
        # unregister/register; iterating the live dict would race.
        for column_name, meta in list(derived.items()):
            if not isinstance(meta, dict):
                continue
            if meta.get("form") != TOKENS_FORM:
                continue
            stored_relpath = meta.get("cache_relpath")
            if stored_relpath == current_relpath:
                # Plan already matches — no realignment needed.
                continue
            source_column = meta.get("source_column")
            model = meta.get("model")
            if not source_column or not model:
                # Malformed metadata — skip rather than crash the load.
                logger.debug(
                    "align: node %r column %r missing source/model; skipping",
                    getattr(node, "name", "<unknown>"),
                    column_name,
                )
                continue
            try:
                # tokenise_column scrubs the old expression (via
                # scrub_plugin_expressions on the matching alias) and
                # stamps a fresh one under ``user_id``'s identity. The
                # derived metadata's ``cache_relpath`` is rewritten to
                # match.
                tokenise_column(
                    node,
                    source_column=str(source_column),
                    model=str(model),
                    language=meta.get("language"),
                    user_id=user_id,
                    workspace_id=str(workspace_id) if workspace_id else None,
                )
                realigned += 1
            except Exception:
                logger.exception(
                    "align: failed to realign tokens for node %r column %r",
                    getattr(node, "name", "<unknown>"),
                    column_name,
                )
    if realigned:
        logger.info(
            "tokens-cache: realigned %d derived column(s) for user %s",
            realigned,
            user_id,
        )
    return realigned


__all__ = ["tokenise_column", "align_tokens_for_current_user"]
