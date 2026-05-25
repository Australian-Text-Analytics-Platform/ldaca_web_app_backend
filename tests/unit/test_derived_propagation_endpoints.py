"""Bug 2 regression: child nodes from filter / slice / sample / concat /
join / clone / expression-apply must inherit the parent's ``derived``
registry. Pre-fix, every Node()-creating endpoint in
``api.workspaces.nodes`` constructed the new node WITHOUT passing
``derived=``, so the child started with an empty registry even when the
LazyFrame still carried the derived column in its schema. Every
downstream tool using ``Node.find_derived_column`` then refused to run
with "re-run Tokenise first".
"""

from __future__ import annotations

from typing import Any, cast

import polars as pl
import pytest
from docworkspace.workspace.core import Workspace
from ldaca_wordflow.api.workspaces import nodes as nodes_api
from ldaca_wordflow.api.workspaces import utils as workspace_utils
from ldaca_wordflow.models import (
    ConcatRequest,
    FilterCondition,
    FilterRequest,
    PolarsExpressionContext,
    PolarsExpressionItem,
    PolarsExpressionRequest,
    SliceRequest,
)

from docworkspace import DerivedColumnMeta, Node

DERIVED_TOKENS_COLUMN = "__derived__.tokens.text.lindera-ja-ipadic"
DERIVED_META: dict[str, Any] = {
    "source_column": "text",
    "form": "tokens",
    "model": "lindera-ja-ipadic",
    "language": "ja",
    "generated_at": "2026-05-14T00:00:00+00:00",
}


def _make_node_with_tokens(name: str = "root") -> Node:
    """Create a Node whose LazyFrame schema includes a registered derived
    tokens column. We don't actually run the tokenizer here — we fabricate
    the derived list-of-struct column directly with ``with_columns`` so the
    test stays under a millisecond and doesn't need to fetch any HF dict.
    """
    df = pl.DataFrame({"text": ["今日は", "良い天気"], "id": [1, 2]}).lazy()
    # Fabricate a list<struct{token, start, end}> column that matches the
    # canonical tokens schema. Per-row content doesn't matter — only the
    # column name + dtype need to survive across filter/slice/etc.
    fake_tokens = df.with_columns(
        pl.struct(
            pl.col("text").alias("token"),
            pl.lit(0, dtype=pl.Int64).alias("start"),
            pl.col("text").str.len_chars().cast(pl.Int64).alias("end"),
        )
        .implode()
        .alias(DERIVED_TOKENS_COLUMN)
    )
    node = Node(
        data=fake_tokens,
        name=name,
        derived={DERIVED_TOKENS_COLUMN: cast(DerivedColumnMeta, dict(DERIVED_META))},
    )
    return node


class _FakeManager:
    """Workspace-manager stub with a real Workspace and real Nodes."""

    def __init__(self, *nodes: Node) -> None:
        self.workspace = Workspace(name="test_ws")
        for node in nodes:
            node.workspace = self.workspace
            self.workspace.add_node(node)
        self.workspace_id = self.workspace.id

    def get_current_workspace(self, _user_id: str):
        return self.workspace

    def get_current_workspace_id(self, _user_id: str):
        return self.workspace_id

    def _resolve_workspace_dir(self, *_args, **_kwargs):  # pragma: no cover
        return "/tmp/dummy"

    def _attach_workspace_dir(self, *_args, **_kwargs):  # pragma: no cover
        return None

    def _set_cached_path(self, *_args, **_kwargs):  # pragma: no cover
        return None

    def save_workspace(self, *_args, **_kwargs):  # pragma: no cover
        return None


@pytest.fixture
def single_parent(monkeypatch: pytest.MonkeyPatch):
    """Workspace with one parent node that already carries a derived entry."""
    parent = _make_node_with_tokens("parent")
    manager = _FakeManager(parent)
    monkeypatch.setattr(nodes_api, "workspace_manager", manager)
    monkeypatch.setattr(workspace_utils, "workspace_manager", manager)
    monkeypatch.setattr(workspace_utils, "update_workspace", lambda *a, **k: None)
    monkeypatch.setattr(nodes_api, "update_workspace", lambda *a, **k: None)
    return manager, parent


@pytest.fixture
def two_parents(monkeypatch: pytest.MonkeyPatch):
    """Workspace with two schema-aligned parents, both carrying derived."""
    parent_a = _make_node_with_tokens("parent_a")
    parent_b = _make_node_with_tokens("parent_b")
    manager = _FakeManager(parent_a, parent_b)
    monkeypatch.setattr(nodes_api, "workspace_manager", manager)
    monkeypatch.setattr(workspace_utils, "workspace_manager", manager)
    monkeypatch.setattr(workspace_utils, "update_workspace", lambda *a, **k: None)
    monkeypatch.setattr(nodes_api, "update_workspace", lambda *a, **k: None)
    return manager, parent_a, parent_b


def _child(manager: _FakeManager, parent_ids: set[str]) -> Node:
    """Find the newest child node added to the workspace, given the parent IDs
    we expect to see in its ``parents`` list.
    """
    for node in manager.workspace.nodes.values():
        if node.id in parent_ids:
            continue
        return node
    raise AssertionError("no child node was added to the workspace")


# ---------------------------------------------------------------------------
# Single-parent, schema-preserving endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clone_inherits_derived(single_parent):
    manager, parent = single_parent

    await nodes_api.clone_node(parent.id, current_user={"id": "user"})

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN in child.derived
    assert child.derived[DERIVED_TOKENS_COLUMN]["model"] == "lindera-ja-ipadic"
    assert child.derived[DERIVED_TOKENS_COLUMN]["language"] == "ja"


@pytest.mark.asyncio
async def test_filter_inherits_derived(single_parent):
    manager, parent = single_parent
    request = FilterRequest(
        conditions=[FilterCondition(column="id", operator="greater_than", value=0)],
    )

    await nodes_api.filter_node(parent.id, request, current_user={"id": "user"})

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN in child.derived
    assert child.derived[DERIVED_TOKENS_COLUMN] == DERIVED_META


@pytest.mark.asyncio
async def test_slice_inherits_derived(single_parent):
    manager, parent = single_parent
    request = SliceRequest(offset=0, length=1)

    await nodes_api.slice_node(parent.id, request, current_user={"id": "user"})

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN in child.derived


@pytest.mark.asyncio
async def test_sample_inherits_derived(single_parent):
    manager, parent = single_parent
    request = SliceRequest(mode="random_sample", sample_size=0.5, random_seed=1)

    await nodes_api.slice_node(parent.id, request, current_user={"id": "user"})

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN in child.derived


# ---------------------------------------------------------------------------
# Multi-parent endpoints
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_concat_inherits_derived_from_parents(two_parents):
    manager, parent_a, parent_b = two_parents
    request = ConcatRequest(node_ids=[parent_a.id, parent_b.id], deduplicate=False)

    await nodes_api.concat_nodes(request, current_user={"id": "user"})

    child = _child(manager, {parent_a.id, parent_b.id})
    assert DERIVED_TOKENS_COLUMN in child.derived
    # Both parents had the same derived entry name; result is one entry.
    assert child.derived[DERIVED_TOKENS_COLUMN]["model"] == "lindera-ja-ipadic"


@pytest.mark.asyncio
async def test_join_inherits_derived_from_both_parents(two_parents):
    manager, parent_a, parent_b = two_parents

    await nodes_api.join_nodes(
        left_node_id=parent_a.id,
        right_node_id=parent_b.id,
        left_on="id",
        right_on="id",
        how="inner",
        current_user={"id": "user"},
    )

    child = _child(manager, {parent_a.id, parent_b.id})
    # Join produces one of the derived columns (right side collides on name);
    # the metadata must still surface so token-mode tools can find it.
    assert DERIVED_TOKENS_COLUMN in child.derived


# ---------------------------------------------------------------------------
# Expression apply — schema-changing variant must filter derived
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expression_apply_with_columns_inherits_derived(single_parent):
    manager, parent = single_parent
    # with_columns + new_node_name forces a new child node (the in-place
    # branch is at L1577 in nodes.py and doesn't go through our fix).
    request = PolarsExpressionRequest(
        context=PolarsExpressionContext.with_columns,
        expressions=[PolarsExpressionItem(code="pl.col('id').alias('id2')")],
        new_node_name="parent_with_id2",
    )

    await nodes_api.polars_expression_apply(
        parent.id, request, current_user={"id": "user"}
    )

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN in child.derived


@pytest.mark.asyncio
async def test_expression_apply_select_drops_derived_if_column_gone(single_parent):
    manager, parent = single_parent
    # A select that keeps only `id` drops the derived tokens column from the
    # output schema. The derived entry pointing at the now-absent column
    # must be filtered out so the child's registry stays consistent with
    # what its LazyFrame actually carries.
    request = PolarsExpressionRequest(
        context=PolarsExpressionContext.select,
        expressions=[PolarsExpressionItem(code="pl.col('id')")],
        new_node_name="parent_only_id",
    )

    await nodes_api.polars_expression_apply(
        parent.id, request, current_user={"id": "user"}
    )

    child = _child(manager, {parent.id})
    assert DERIVED_TOKENS_COLUMN not in child.derived
    assert child.derived == {}
