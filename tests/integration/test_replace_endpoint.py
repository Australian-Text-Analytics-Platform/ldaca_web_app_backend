import polars as pl
import pytest

from ldaca_web_app_backend.api.workspaces import nodes as nodes_api


class _DummyNode:
    def __init__(self, frame: pl.DataFrame):
        self.data = frame.lazy()
        self.name = "dummy"


class _DummyWorkspace:
    def __init__(self, persist_calls: dict[str, int], nodes=None):
        self.name = "ws"
        self._persist_calls = persist_calls
        self.nodes = nodes or {}

    def set_metadata(self, *_args, **_kwargs):
        return None

    def save(self, *_args, **_kwargs):
        self._persist_calls["count"] += 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_replace_preview_returns_masked_values(authenticated_client, monkeypatch):
    frame = pl.DataFrame({"Body": ["Invoice 123", "Order 987"]})
    node = _DummyNode(frame)
    workspace_id = "ws-alpha"
    dummy_ws = _DummyWorkspace({"count": 0}, nodes={"node-123": node})

    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "get_current_workspace_id",
        lambda user_id: workspace_id,
    )
    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "get_current_workspace",
        lambda _user_id: dummy_ws,
    )

    response = await authenticated_client.post(
        "/api/workspaces/nodes/node-123/replace/preview",
        json={
            "source_column": "Body",
            "pattern": r"\d+",
            "replacement": "#",
            "output_column_name": "Body_masked",
            "preview_limit": 2,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data"][0]["Body_masked"] == "Invoice #"
    assert payload["data"][1]["Body_masked"] == "Order #"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_replace_apply_mutates_node_data(authenticated_client, monkeypatch):
    frame = pl.DataFrame({"Body": ["Invoice 123", "Order 987"]})
    node = _DummyNode(frame)
    workspace_id = "ws-alpha"
    persist_calls = {"count": 0}
    dummy_ws = _DummyWorkspace(persist_calls, nodes={"node-123": node})

    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "get_current_workspace_id",
        lambda user_id: workspace_id,
    )
    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "get_current_workspace",
        lambda user_id: dummy_ws,
    )
    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "_resolve_workspace_dir",
        lambda user_id, workspace_id, workspace_name: "/tmp/ws",
    )
    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "_attach_workspace_dir",
        lambda workspace, path: None,
    )
    monkeypatch.setattr(
        nodes_api.workspace_manager,
        "_set_cached_path",
        lambda user_id, workspace_id, path: None,
    )

    response = await authenticated_client.post(
        "/api/workspaces/nodes/node-123/replace",
        json={
            "source_column": "Body",
            "pattern": r"\d+",
            "replacement": "#",
            "output_column_name": "Body_masked",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["column_name"] == "Body_masked"
    assert payload["state"] == "successful"
    collected = node.data.collect()
    assert isinstance(collected, pl.DataFrame)
    masked_values = collected.get_column("Body_masked").to_list()
    assert masked_values == ["Invoice #", "Order #"]
    assert persist_calls["count"] == 1
