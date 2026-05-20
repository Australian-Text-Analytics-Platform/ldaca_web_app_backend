"""Phase 2.5 — auto-migrate eager hash-join plans to the lazy expression.

When `LDACA_LAZY_TOKENISE=1`, every workspace load runs this pass after
:func:`Workspace.load` returns. It walks the workspace's nodes and, for
each tokenised derived column whose plan is still in the OLD eager
hash-join shape (built by ``derived_columns._build_cache_join``),
overlays a ``polars_text.tokenize_with_cache_lookup`` expression onto
the same column name — so subsequent collects materialise tokens via
the lazy expression rather than via the eager cache parquet read.

Why we keep the old hash-join in the plan instead of surgically
removing it: polars' plan AST is internal; removing a join requires
either FFI surgery (too invasive for Phase 2.5) or reconstructing the
base plan from scratch (we don't store the pre-tokenise plan
separately). Adding the lazy expression on top with the same alias
SHADOWS the eager output, which is functionally correct — the lazy
column wins; the eager hash-join is computed but its derived-column
output is overwritten. After a user re-tokenises (which goes through
``tokenise_column``'s lazy path, building a clean lazy plan from
scratch), the wasted work disappears. Phase 4.5's full retirement of
the eager infrastructure assumes this transition has run its course.

Migration is one-way and idempotent: once a derived column's metadata
gains ``"plan_shape": "lazy_v1"`` we skip it on subsequent loads.

See: backend/docs/developer-guide/lazy-tokenisation-refactor.md §8.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from . import tokens_cache

logger = logging.getLogger(__name__)


# Marker stored in `node.derived[col]` to signal "plan is already in
# the lazy shape, skip migration on next load". Versioned so a future
# plan format change can trigger a re-migration without re-walking
# every plan shape.
PLAN_SHAPE_KEY = "plan_shape"
PLAN_SHAPE_LAZY_V1 = "lazy_v1"

# The form value `tokenise_column` writes for tokenisation-derived
# columns. Kept in sync with ``api.workspaces.analyses.generated_columns.TOKENS_FORM``
# but imported lazily inside functions so the module-level import graph
# stays minimal (this module is loaded once per workspace open, in the
# load hot path).


def _model_is_case_free(model: str) -> bool:
    # Mirrors `derived_columns._CASE_FREE_MODELS` without taking a hard
    # dep on that module's import surface (lets this module load even
    # if `derived_columns` has a forward-ref issue during partial
    # startup).
    return model in {
        "jieba",
        "lindera-ja-ipadic",
        "lindera-ja-unidic",
        "lindera-ko-dic",
    }


def migrate_workspace_to_lazy(workspace: Any) -> int:
    """Walk a freshly-loaded workspace and migrate eager plans to lazy.

    Returns the number of derived columns that were migrated. No-op when
    the workspace has no tokenised nodes or every tokenised column is
    already in the lazy shape. Caller is expected to gate this by
    checking ``derived_columns._lazy_tokenise_enabled()`` so the lazy
    expression's runtime requirements (``LDACA_TOKENS_CACHE_DIR`` env
    var resolution) match what was used to build the migrated plan.

    Failures inside the per-node walk are logged and swallowed — a
    migration error on ONE node must never block opening the rest of
    the workspace. The repair pass continues to handle anything this
    walk misses.
    """
    nodes = _iter_nodes(workspace)
    migrated = 0
    for node in nodes:
        try:
            migrated += _migrate_node(node)
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "tokens-lazy-migration: failed for node %r: %s",
                getattr(node, "name", "<unknown>"),
                exc,
                exc_info=True,
            )
    if migrated:
        logger.info(
            "tokens-lazy-migration: migrated %d derived column(s) to lazy_v1",
            migrated,
        )
    return migrated


def _iter_nodes(workspace: Any) -> Iterable[Any]:
    """Yield every Node in the workspace, tolerating shape changes.

    docworkspace exposes ``workspace.nodes`` as a dict-like; we don't
    constrain it tighter here so a future container swap (list, set,
    keyed views) doesn't break the migration walk.
    """
    nodes_attr = getattr(workspace, "nodes", None)
    if nodes_attr is None:
        return ()
    if hasattr(nodes_attr, "values"):
        return nodes_attr.values()
    return nodes_attr


def _migrate_node(node: Any) -> int:
    """Migrate every eligible derived column on one node. Returns the
    count of columns rewritten."""
    # Lazy imports so this module can load without dragging the API
    # layer into the cycle (api/workspaces -> core/workspace -> here).
    from ..api.workspaces.analyses.generated_columns import TOKENS_FORM
    from .derived_columns import _REMOVE_PUNCT_DEFAULT, _build_lazy_plan

    derived: dict = getattr(node, "derived", None) or {}
    if not derived:
        return 0

    # Snapshot keys so per-column rewrites don't mutate the dict we're
    # iterating over (registering/unregistering derived metadata
    # mid-walk would break iteration).
    candidates: list[tuple[str, dict]] = []
    for derived_name, meta in derived.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("form") != TOKENS_FORM:
            continue
        if meta.get(PLAN_SHAPE_KEY) == PLAN_SHAPE_LAZY_V1:
            continue
        candidates.append((derived_name, meta))

    if not candidates:
        return 0

    user_id = _user_id_for_node(node)
    if user_id is None:
        # No user_id binding on the node → the lazy expression can't
        # resolve its cache directory. Bail rather than guess.
        logger.debug(
            "tokens-lazy-migration: node %r has no user_id binding; skipping",
            getattr(node, "name", "<unknown>"),
        )
        return 0

    migrated_here = 0
    for derived_name, meta in candidates:
        source_column = meta.get("source_column")
        model = meta.get("model")
        if not (source_column and model):
            logger.debug(
                "tokens-lazy-migration: derived %r on node %r missing source/model; skipping",
                derived_name,
                getattr(node, "name", "<unknown>"),
            )
            continue
        params = {
            "lowercase": not _model_is_case_free(model),
            "remove_punct": _REMOVE_PUNCT_DEFAULT,
        }
        bucket_filename = tokens_cache.cache_filename(model, params)

        # The shadow-overlay approach: keep the OLD hash-join plan
        # intact under the same alias, and add the lazy expression on
        # top with the same name so `with_columns` overwrites the
        # column. The eager hash-join still executes but its output is
        # superseded — see module docstring for the rationale.
        try:
            new_lf = _build_lazy_plan(
                base_lf=node.data,
                source_column=source_column,
                user_id=user_id,
                model=model,
                params=params,
                bucket_filename=bucket_filename,
                derived_name=derived_name,
            )
        except Exception as exc:
            logger.warning(
                "tokens-lazy-migration: build_lazy_plan failed for %r on %r: %s",
                derived_name,
                getattr(node, "name", "<unknown>"),
                exc,
            )
            continue

        node.data = new_lf
        meta[PLAN_SHAPE_KEY] = PLAN_SHAPE_LAZY_V1
        migrated_here += 1

    return migrated_here


def _user_id_for_node(node: Any) -> str | None:
    """Resolve the user_id this node belongs to.

    The Node object itself doesn't typically carry a ``user_id`` —
    workspaces are loaded by a known user, and that's the migration's
    user context. The migrator passes user_id explicitly via
    :func:`migrate_workspace_to_lazy_for_user`; callers that don't have
    a per-user binding (rare) fall through to ``None`` and skip
    migration.
    """
    # Inline-set context (the for_user variant below stashes it on the
    # nodes it processes). Falls back to a ``_user_id`` attribute the
    # caller may have set on the Node itself for compatibility.
    return getattr(node, "_ldaca_lazy_migration_user", None) or getattr(
        node, "_user_id", None
    )


def migrate_workspace_to_lazy_for_user(workspace: Any, user_id: str) -> int:
    """Like :func:`migrate_workspace_to_lazy` but binds the user_id
    context onto each node before walking, so the lazy expression's
    cache-dir resolution lands on the right per-user subdir.

    The user_id is removed after the walk so we don't leave an
    incidental attribute on the Node objects that survives into
    serialisation (docworkspace serialises via ``__dict__``).
    """
    nodes = list(_iter_nodes(workspace))
    for node in nodes:
        # Stash; restore in `finally`. Persisting this attribute past the
        # call would surface in `node.__dict__` and confuse downstream
        # serialisers that aren't aware of our migration plumbing.
        node._ldaca_lazy_migration_user = user_id  # type: ignore[attr-defined]
    try:
        return migrate_workspace_to_lazy(workspace)
    finally:
        for node in nodes:
            try:
                delattr(node, "_ldaca_lazy_migration_user")
            except AttributeError:
                pass


__all__ = [
    "PLAN_SHAPE_KEY",
    "PLAN_SHAPE_LAZY_V1",
    "migrate_workspace_to_lazy",
    "migrate_workspace_to_lazy_for_user",
]
