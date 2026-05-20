"""Phase 2.5 — `migrate_workspace_to_lazy_for_user` rewrites eager
hash-join plans to the lazy expression.

The migration overlays a `polars_text.tokenize_with_cache_lookup`
expression on top of each tokenised node's plan, and tags the node's
derived metadata with `plan_shape: lazy_v1` so subsequent loads skip
the migration. The eager hash-join in the plan is intentionally NOT
removed (see module docstring) — the lazy expression's output shadows
it via the same column alias.

Coverage:

* migration walks tokenised nodes and adds the lazy shape on top
* tagged metadata blocks re-migration on the next load (idempotency)
* mixed workspaces (some tokenised + some not) only touch tokenised nodes
* nodes whose derived metadata is missing source_column / model are
  skipped rather than crashing the workspace open
* node-level exceptions don't propagate (one bad node ≠ whole load fail)

End-to-end portability (.plbin round-trip across machines) is covered
by the Phase 2 lazy-path tests; this suite focuses on the migration
walker itself.

See: backend/docs/developer-guide/lazy-tokenisation-refactor.md §8.
"""

from __future__ import annotations

from typing import Any

import polars as pl
import pytest
from docworkspace import Node

from ldaca_wordflow.api.workspaces.analyses.generated_columns import TOKENS_FORM
from ldaca_wordflow.core.derived_columns import LAZY_TOKENISE_ENV, tokenise_column
from ldaca_wordflow.core.tokens_lazy_migration import (
    PLAN_SHAPE_KEY,
    PLAN_SHAPE_LAZY_V1,
    migrate_workspace_to_lazy,
    migrate_workspace_to_lazy_for_user,
)


def _make_node(name: str = "root") -> Node:
    df = pl.DataFrame(
        {
            "text": ["alpha beta", "gamma delta", "epsilon zeta"],
            "value": [1, 2, 3],
        }
    ).lazy()
    return Node(data=df, name=name)


class _MockWorkspace:
    """Minimal stand-in for the docworkspace `Workspace.nodes` shape —
    the migration walker only needs `.nodes` as a values-iterable."""

    def __init__(self, nodes: dict[str, Node]) -> None:
        self.nodes = nodes


@pytest.fixture
def lazy_off(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensures the eager path runs for setup (flag must be off when we
    tokenise the seed nodes so they end up in the OLD shape we want to
    migrate from)."""
    monkeypatch.delenv(LAZY_TOKENISE_ENV, raising=False)


def _tokenise_eager(node: Node, *, user_id: str = "test_user_lazy_mig") -> str:
    return tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id=user_id,
    )


def test_migration_walks_eager_tokenised_node(lazy_off: None) -> None:
    node = _make_node("seed")
    derived_name = _tokenise_eager(node)
    meta = node.derived[derived_name]
    # Sanity check — eager path did NOT mark plan_shape
    assert PLAN_SHAPE_KEY not in meta

    ws = _MockWorkspace({"seed": node})
    migrated = migrate_workspace_to_lazy_for_user(ws, "test_user_lazy_mig")
    assert migrated == 1
    assert meta[PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1

    # The node's data plan still has the derived column (and still
    # resolves to a List<Struct<...>> when collected — proving the
    # lazy expression took over via the same alias).
    schema = node.data.collect_schema()
    assert derived_name in schema.names()
    dtype = schema[derived_name]
    assert isinstance(dtype, pl.List)
    inner = dtype.inner
    assert isinstance(inner, pl.Struct)
    assert {f.name for f in inner.fields} == {"token", "start", "end"}


def test_migration_is_idempotent(lazy_off: None) -> None:
    node = _make_node("seed-idem")
    derived_name = _tokenise_eager(node)
    ws = _MockWorkspace({"seed-idem": node})

    first = migrate_workspace_to_lazy_for_user(ws, "test_user_lazy_mig")
    assert first == 1
    plan_before_second = node.data
    second = migrate_workspace_to_lazy_for_user(ws, "test_user_lazy_mig")
    assert second == 0, "tagged node must not be re-migrated"
    # Plan reference unchanged (no rewrite happened)
    assert node.data is plan_before_second
    # Marker still set
    assert node.derived[derived_name][PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1


def test_migration_skips_non_tokenised_nodes(lazy_off: None) -> None:
    # A node that was never tokenised should be untouched. Plan
    # identity must hold (no needless rewrites).
    node = _make_node("plain")
    ws = _MockWorkspace({"plain": node})
    plan_before = node.data
    migrated = migrate_workspace_to_lazy_for_user(ws, "test_user_lazy_mig")
    assert migrated == 0
    assert node.data is plan_before


def test_migration_mixed_workspace(lazy_off: None) -> None:
    # Two tokenised nodes (different models) + one untouched.
    a = _make_node("tok-a")
    _tokenise_eager(a, user_id="mix_user")
    b = _make_node("tok-b")
    tokenise_column(
        b,
        source_column="text",
        model="jieba",  # CJK / case-free path — different params hash
        language="zh",
        user_id="mix_user",
    )
    c = _make_node("plain")

    ws = _MockWorkspace({"tok-a": a, "tok-b": b, "plain": c})
    migrated = migrate_workspace_to_lazy_for_user(ws, "mix_user")
    assert migrated == 2, f"expected 2 tokenised migrations, got {migrated}"
    # Both tokenised nodes now tagged; plain node untouched
    a_derived = [v for v in a.derived.values() if v.get("form") == TOKENS_FORM]
    b_derived = [v for v in b.derived.values() if v.get("form") == TOKENS_FORM]
    assert all(m[PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1 for m in a_derived)
    assert all(m[PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1 for m in b_derived)
    assert not c.derived


def test_migration_skips_metadata_with_missing_fields(lazy_off: None) -> None:
    # If a derived entry is malformed (no model / no source_column),
    # the migrator must skip rather than crash.
    node = _make_node("malformed")
    _tokenise_eager(node, user_id="malformed_user")
    derived_name = next(iter(node.derived.keys()))
    # Corrupt the metadata: strip source_column
    node.derived[derived_name].pop("source_column", None)

    ws = _MockWorkspace({"malformed": node})
    migrated = migrate_workspace_to_lazy_for_user(ws, "malformed_user")
    assert migrated == 0, "malformed metadata must NOT be migrated"
    assert PLAN_SHAPE_KEY not in node.derived[derived_name]


def test_migration_per_node_failure_does_not_block_others(
    lazy_off: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Two tokenised nodes: monkeypatch the build to fail on the FIRST.
    # The second must still migrate.
    n1 = _make_node("fails")
    _tokenise_eager(n1, user_id="resilience_user")
    n2 = _make_node("ok")
    _tokenise_eager(n2, user_id="resilience_user")

    import ldaca_wordflow.core.tokens_lazy_migration as mig

    original = mig._migrate_node
    call_count = {"n": 0}

    def _flaky_migrate(node: Any) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated failure on first node")
        return original(node)

    monkeypatch.setattr(mig, "_migrate_node", _flaky_migrate)

    ws = _MockWorkspace({"fails": n1, "ok": n2})
    # Should NOT raise. The walker catches and logs per-node errors.
    migrate_workspace_to_lazy_for_user(ws, "resilience_user")

    # n1 not migrated, n2 IS
    n1_meta = next(
        v for v in n1.derived.values() if v.get("form") == TOKENS_FORM
    )
    n2_meta = next(
        v for v in n2.derived.values() if v.get("form") == TOKENS_FORM
    )
    assert PLAN_SHAPE_KEY not in n1_meta
    assert n2_meta[PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1


def test_migration_empty_workspace_is_noop() -> None:
    ws = _MockWorkspace({})
    migrated = migrate_workspace_to_lazy_for_user(ws, "anyone")
    assert migrated == 0


def test_migrate_workspace_without_user_id_skips() -> None:
    # Calling the no-user variant on a node missing the binding skips
    # rather than guessing. Exercises the safety net in `_user_id_for_node`.
    node = _make_node("no-user")
    # Set up derived metadata that would otherwise be eligible
    node.derived[node.name + ".tokens"] = {
        "source_column": "text",
        "form": TOKENS_FORM,
        "model": "bert-base-uncased",
        "language": "en",
    }
    ws = _MockWorkspace({"no-user": node})
    # Direct call (not the _for_user variant) — no user_id stashed
    migrated = migrate_workspace_to_lazy(ws)
    assert migrated == 0
    assert PLAN_SHAPE_KEY not in node.derived[node.name + ".tokens"]


def test_migration_attribute_cleanup_after_walk(lazy_off: None) -> None:
    # The _for_user variant stashes user_id as a private attribute on
    # each node during the walk, then removes it. Verify cleanup so
    # docworkspace's __dict__-based serialiser doesn't see stale state.
    node = _make_node("cleanup")
    _tokenise_eager(node, user_id="cleanup_user")
    ws = _MockWorkspace({"cleanup": node})
    migrate_workspace_to_lazy_for_user(ws, "cleanup_user")
    assert not hasattr(node, "_ldaca_lazy_migration_user")


def test_preflight_skips_when_lazy_flag_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When LDACA_LAZY_TOKENISE is on, `assert_tokens_available_for_nodes`
    must short-circuit without touching the per-node walk that would
    otherwise raise on empty/missing cache parquets. The lazy expression
    treats missing files as cache misses, so the "missing tokens"
    failure mode is unreachable for freshly lazy nodes and shadowed
    for migrated ones — surfacing it would be incorrect."""
    from ldaca_wordflow.api.workspaces.utils import (
        assert_tokens_available_for_nodes,
    )

    # Sentinel that would explode if the function tried to deref it
    class _Workspace:
        ws_root_dir = None
        nodes = {"some-node": object()}

    monkeypatch.setenv(LAZY_TOKENISE_ENV, "1")
    # Should NOT raise — the gate skips the detection walk entirely.
    assert_tokens_available_for_nodes(
        _Workspace(), ["some-node"], action="test analysis"
    )


def test_banner_state_returns_none_when_lazy_flag_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The /graph endpoint attaches `tokens_cache_repair` via
    `_runtime_tokens_cache_state`. Under the lazy flag the banner makes
    no sense (empty cache is the lazy expression's normal state), so
    the helper returns None and the field is omitted from the response."""
    from ldaca_wordflow.api.workspaces.lifecycle import (
        _runtime_tokens_cache_state,
    )

    class _Workspace:
        ws_root_dir = None
        nodes = {}

    monkeypatch.setenv(LAZY_TOKENISE_ENV, "1")
    assert _runtime_tokens_cache_state(_Workspace()) is None


def test_phase2_tokenise_with_flag_on_tags_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The flag-on tokenise path should pre-tag the metadata so the
    # subsequent load's migration walker skips immediately.
    monkeypatch.setenv(LAZY_TOKENISE_ENV, "1")
    node = _make_node("preflagged")
    derived_name = tokenise_column(
        node,
        source_column="text",
        model="bert-base-uncased",
        language="en",
        user_id="preflag_user",
    )
    meta = node.derived[derived_name]
    assert meta[PLAN_SHAPE_KEY] == PLAN_SHAPE_LAZY_V1

    # Migration walker is a no-op on a pre-tagged node
    ws = _MockWorkspace({"preflagged": node})
    migrated = migrate_workspace_to_lazy_for_user(ws, "preflag_user")
    assert migrated == 0
