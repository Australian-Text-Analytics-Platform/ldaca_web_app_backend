"""Pin the workspace-load alignment hook for cross-user-imported plans.

When ``WorkspaceManager.set_current_workspace`` loads a workspace, it
calls ``align_tokens_for_current_user(ws, current_user_id)``. For every
node with a tokens-form derived column whose stored ``cache_relpath``
doesn't match the *current* user's expected one, the hook:

1. scrubs the old ``tokenize_with_cache_lookup`` expression from the
   lazy plan (via ``polars_text.scrub_plugin_expressions``), and
2. re-stamps a fresh expression bound to the current user's identity.

The end-to-end safety contract: a workspace authored by user A and
imported by user B has B's cache-relpath in the plan AFTER load, so
subsequent analyses write to B's tree — never A's.

We compare cache *paths*, not user-id strings: two users with the
same id on different OSes resolve to different paths, and a single
user moving from single-user to multi-user mode also crosses a
boundary that needs the realignment.
"""

from __future__ import annotations

import polars as pl
import pytest
from docworkspace import Node

from ldaca_wordflow import settings as _settings_module
from ldaca_wordflow.api.workspaces.analyses.generated_columns import (
    TOKENS_FORM,
    derived_column_name,
)
from ldaca_wordflow.core import tokens_cache as tc
from ldaca_wordflow.core.derived_columns import (
    align_tokens_for_current_user,
    tokenise_column,
)


@pytest.fixture(autouse=True)
def isolated_cache_root(tmp_path, monkeypatch):
    monkeypatch.setenv(tc.CACHE_ROOT_ENV, str(tmp_path / "tokens-cache"))
    yield tmp_path / "tokens-cache"


@pytest.fixture
def multi_user_mode(monkeypatch):
    """Force multi-user mode so different ``user_id``s resolve to
    distinct cache relpaths (``user_<id>/user_cache`` instead of the
    shared ``user_root/user_cache``)."""
    monkeypatch.setattr(_settings_module.settings, "multi_user", True)
    yield


class _MockWorkspace:
    """Stand-in for docworkspace.Workspace — the alignment hook only
    needs ``.nodes`` as a values-iterable and an ``.id`` attribute."""

    def __init__(self, nodes: dict[str, Node], workspace_id: str = "ws-1") -> None:
        self.nodes = nodes
        self.id = workspace_id


def _make_node(name: str = "root") -> Node:
    return Node(
        data=pl.DataFrame({"text": ["alpha", "beta"]}).lazy(),
        name=name,
    )


def test_align_rewrites_plan_when_cache_relpath_mismatches(
    multi_user_mode, isolated_cache_root
):
    """Crux of the contract: a workspace tokenised by Alice and
    imported by Bob must have its lazy plan's user_id kwarg
    rewritten to Bob's relpath before any analysis runs."""
    node = _make_node("imported-node")
    tokenise_column(
        node,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )
    column_name = derived_column_name(TOKENS_FORM, "text", "jieba")
    alice_relpath = tc.cache_relpath_for_user("alice")
    bob_relpath = tc.cache_relpath_for_user("bob")
    assert alice_relpath != bob_relpath  # sanity for multi-user mode

    assert node.derived[column_name]["cache_relpath"] == alice_relpath
    assert alice_relpath.encode() in node.data.serialize(format="binary")

    ws = _MockWorkspace({"n": node})
    realigned = align_tokens_for_current_user(ws, "bob")
    assert realigned == 1

    # Plan now carries bob's relpath; alice's is scrubbed out.
    blob_after = node.data.serialize(format="binary")
    assert bob_relpath.encode() in blob_after
    assert alice_relpath.encode() not in blob_after
    # Metadata mirrors the new identity.
    assert node.derived[column_name]["cache_relpath"] == bob_relpath


def test_align_is_noop_when_cache_relpath_already_matches(
    multi_user_mode, isolated_cache_root
):
    """The common case: the current user is the original author."""
    node = _make_node("own-node")
    tokenise_column(
        node,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )
    plan_before = node.data
    ws = _MockWorkspace({"n": node})
    assert align_tokens_for_current_user(ws, "alice") == 0
    # Identity holds — no plan rewrite means downstream lazy work
    # that holds a reference to the previous LF stays valid.
    assert node.data is plan_before


def test_align_is_noop_on_non_tokenised_nodes(isolated_cache_root):
    """Nodes with no tokens-form derived columns are skipped entirely."""
    node = _make_node("plain")
    plan_before = node.data
    ws = _MockWorkspace({"plain": node})
    assert align_tokens_for_current_user(ws, "anyone") == 0
    assert node.data is plan_before


def test_align_skips_malformed_metadata(isolated_cache_root):
    """A derived entry missing source_column or model (corrupted
    metadata or partial-write recovery) is skipped — load must not
    crash on a single bad column."""
    node = _make_node("corrupt")
    node.derived[derived_column_name(TOKENS_FORM, "text", "jieba")] = {
        "form": TOKENS_FORM,
        # missing source_column and model
        "language": "zh",
        "cache_relpath": "user_someone/user_cache",
    }
    plan_before = node.data
    ws = _MockWorkspace({"corrupt": node})
    assert align_tokens_for_current_user(ws, "anyone") == 0
    assert node.data is plan_before


def test_align_continues_when_one_node_fails(
    multi_user_mode, isolated_cache_root, monkeypatch
):
    """A single node that crashes tokenise_column must not block the
    rest of the workspace."""
    n1 = _make_node("fails")
    tokenise_column(
        n1,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )
    n2 = _make_node("ok")
    tokenise_column(
        n2,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )

    import ldaca_wordflow.core.derived_columns as dc_module

    original = dc_module.tokenise_column
    call = {"n": 0}

    def flaky(*args, **kwargs):
        call["n"] += 1
        if call["n"] == 1:
            raise RuntimeError("simulated failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(dc_module, "tokenise_column", flaky)

    ws = _MockWorkspace({"fails": n1, "ok": n2})
    # Must not raise; the surviving node still gets realigned.
    align_tokens_for_current_user(ws, "bob")
    assert call["n"] == 2


def test_align_only_touches_tokens_form_derived_columns(
    multi_user_mode, isolated_cache_root
):
    """A node may carry non-tokens derived metadata (e.g. embeddings
    in a future analysis). Alignment must leave those entries
    untouched."""
    node = _make_node("mixed")
    tokenise_column(
        node,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )
    node.derived["__derived__.embeddings.text.dummy"] = {
        "form": "embeddings",
        "source_column": "text",
        "model": "dummy",
    }
    ws = _MockWorkspace({"mixed": node})
    assert align_tokens_for_current_user(ws, "bob") == 1
    assert "__derived__.embeddings.text.dummy" in node.derived


def test_align_empty_workspace_is_noop(isolated_cache_root):
    ws = _MockWorkspace({})
    assert align_tokens_for_current_user(ws, "anyone") == 0


def test_align_handles_missing_cache_relpath_in_metadata(
    multi_user_mode, isolated_cache_root
):
    """Pre-existing workspaces saved before ``cache_relpath`` was
    added to the metadata schema must still be realigned. A missing
    field reads as ``None``, which never equals the current user's
    relpath, so the hook treats it as a mismatch and re-stamps."""
    node = _make_node("legacy")
    tokenise_column(
        node,
        source_column="text",
        model="jieba",
        language="zh",
        user_id="alice",
        workspace_id="ws-1",
    )
    column_name = derived_column_name(TOKENS_FORM, "text", "jieba")
    # Simulate an old plbin: drop ``cache_relpath`` from the metadata.
    del node.derived[column_name]["cache_relpath"]

    ws = _MockWorkspace({"n": node})
    assert align_tokens_for_current_user(ws, "alice") == 1
    # Realignment now stamps the relpath the new code expects.
    assert (
        node.derived[column_name]["cache_relpath"]
        == tc.cache_relpath_for_user("alice")
    )
