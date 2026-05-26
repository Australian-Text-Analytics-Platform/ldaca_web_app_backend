"""Phase 2.5: tokenization API endpoint integration tests.

Exercises ``POST /workspaces/nodes/{node_id}/derived/tokens`` by calling the
endpoint handler directly with a monkey-patched ``workspace_manager``.
The endpoint logic itself is thin (auth + lookup + delegate to
``tokenise_column``) so this test ensures:

- 200 with ``is_new=True`` on first call,
- 200 with ``is_new=False`` and ``replaced_column`` on repeat same-source call,
- 200 with ``is_new=True`` on a different source column,
- 400 on missing source column,
- 404 on unknown node / no active workspace.
"""

from __future__ import annotations

import polars as pl
import pytest
from fastapi import HTTPException
from ldaca_wordflow.api.workspaces import utils as workspace_utils
from ldaca_wordflow.api.workspaces.analyses import derived_columns as derived_api
from ldaca_wordflow.api.workspaces.analyses.derived_columns import (
    TokeniseColumnRequest,
)

from docworkspace import Node


@pytest.fixture
def make_node():
    def _make(name: str = "root") -> Node:
        df = pl.DataFrame(
            {"text": ["hello world", "goodbye world"], "value": [1, 2]}
        ).lazy()
        return Node(data=df, name=name)

    return _make


@pytest.fixture
def fake_workspace_manager(monkeypatch: pytest.MonkeyPatch, make_node):
    """Provides a no-op workspace_manager carrying a single Node by id."""
    node = make_node()

    class _Workspace:
        id = "ws1"
        name = "dummy"
        ws_root_dir = "/tmp/dummy"

        def __init__(self):
            self.nodes = {node.id: node}

        def save(self, _target_dir):  # pragma: no cover - never invoked here
            return None

    workspace = _Workspace()

    class _Manager:
        def get_current_workspace(self, _user_id: str):
            return workspace

        def get_current_workspace_id(self, _user_id: str):
            return workspace.id

        def _resolve_workspace_dir(self, *_args, **_kwargs):  # pragma: no cover
            return "/tmp/dummy"

        def _attach_workspace_dir(self, *_args, **_kwargs):  # pragma: no cover
            return None

        def _set_cached_path(self, *_args, **_kwargs):  # pragma: no cover
            return None

    manager = _Manager()
    monkeypatch.setattr(derived_api, "workspace_manager", manager)
    monkeypatch.setattr(workspace_utils, "workspace_manager", manager)
    # Stub out persistence so the test doesn't try to write plbin to /tmp/dummy.
    monkeypatch.setattr(workspace_utils, "update_workspace", lambda *a, **k: None)
    monkeypatch.setattr(derived_api, "update_workspace", lambda *a, **k: None)
    return manager, workspace, node


@pytest.mark.asyncio
async def test_post_tokens_creates_new_column(fake_workspace_manager):
    _manager, _workspace, node = fake_workspace_manager
    request = TokeniseColumnRequest(
        source_column="text", model="bert-base-uncased", language="en"
    )

    result = await derived_api.create_derived_tokens(
        node_id=node.id, request=request, current_user={"id": "user"}
    )

    assert result.is_new is True
    assert result.replaced_column is None
    assert result.column == "tokenization.text.bert-base-uncased"
    assert node.tokenization["text"]["column_name"] == result.column
    assert result.column not in node.data.collect_schema().names()
    assert node.tokenization["text"]["cache_backend"] == "duckdb"


@pytest.mark.asyncio
async def test_post_tokens_replays_replaces_existing(fake_workspace_manager):
    _manager, _workspace, node = fake_workspace_manager
    request = TokeniseColumnRequest(
        source_column="text", model="bert-base-uncased", language="en"
    )

    first = await derived_api.create_derived_tokens(
        node_id=node.id, request=request, current_user={"id": "user"}
    )
    second = await derived_api.create_derived_tokens(
        node_id=node.id, request=request, current_user={"id": "user"}
    )

    assert second.is_new is False
    assert second.replaced_column == first.column
    assert second.column == first.column
    assert len(node.tokenization) == 1


@pytest.mark.asyncio
async def test_post_tokens_different_source_preserves_existing_token_specs(
    fake_workspace_manager,
):
    _manager, _workspace, node = fake_workspace_manager

    first = await derived_api.create_derived_tokens(
        node_id=node.id,
        request=TokeniseColumnRequest(
            source_column="text", model="bert-base-uncased", language="en"
        ),
        current_user={"id": "user"},
    )
    second = await derived_api.create_derived_tokens(
        node_id=node.id,
        request=TokeniseColumnRequest(
            source_column="value", model="jieba", language="zh"
        ),
        current_user={"id": "user"},
    )

    assert second.is_new is True
    assert second.replaced_column is None
    assert node.find_tokenization_column("text") == first.column
    assert second.column == "tokenization.value.jieba"
    assert node.find_tokenization_column("value") == second.column
    assert len(node.tokenization) == 2


@pytest.mark.asyncio
async def test_post_tokens_400_on_missing_source(fake_workspace_manager):
    _manager, _workspace, node = fake_workspace_manager
    request = TokeniseColumnRequest(
        source_column="nonexistent", model="bert-base-uncased", language="en"
    )

    with pytest.raises(HTTPException) as exc_info:
        await derived_api.create_derived_tokens(
            node_id=node.id, request=request, current_user={"id": "user"}
        )
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_post_tokens_404_on_unknown_node(fake_workspace_manager):
    request = TokeniseColumnRequest(
        source_column="text", model="bert-base-uncased", language="en"
    )

    with pytest.raises(HTTPException) as exc_info:
        await derived_api.create_derived_tokens(
            node_id="does-not-exist", request=request, current_user={"id": "user"}
        )
    assert exc_info.value.status_code == 404
