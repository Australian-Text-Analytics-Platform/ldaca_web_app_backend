"""Tests for the workspace UI-state sidecar endpoints.

Drive both handlers directly with a monkey-patched ``workspace_manager``
that maps workspace_id → a tmp_path. The endpoints are tiny — load /
save a JSON sidecar at ``<workspace_dir>/ui_state.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi import HTTPException
from ldaca_wordflow.api.workspaces import ui_state as ui_state_api


def _state(
    payload: dict[str, dict[str, str]] | None = None,
) -> ui_state_api.WorkspaceUiState:
    return ui_state_api.WorkspaceUiState.model_validate(payload or {})


class _FakeManager:
    def __init__(self, workspace_id: str, workspace_dir: Path):
        self.workspace_id = workspace_id
        self.workspace_dir = workspace_dir

    def get_workspace_dir(self, _user_id: str, workspace_id: str):
        if workspace_id != self.workspace_id:
            return None
        return self.workspace_dir


@pytest.fixture
def fake_workspace(tmp_path, monkeypatch):
    manager = _FakeManager(workspace_id="ws1", workspace_dir=tmp_path)
    monkeypatch.setattr(ui_state_api, "workspace_manager", manager)
    return manager


@pytest.mark.asyncio
async def test_get_returns_empty_object_when_file_missing(fake_workspace):
    result = await ui_state_api.get_workspace_ui_state(
        workspace_id="ws1", current_user={"id": "u"}
    )
    assert result == ui_state_api.WorkspaceUiState()


@pytest.mark.asyncio
async def test_get_returns_parsed_contents_when_file_present(fake_workspace):
    payload = {"node_colors": {"node-a": "#2563eb"}}
    (fake_workspace.workspace_dir / "ui_state.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    result = await ui_state_api.get_workspace_ui_state(
        workspace_id="ws1", current_user={"id": "u"}
    )
    assert result.model_dump() == payload


@pytest.mark.asyncio
async def test_get_404s_on_unknown_workspace(fake_workspace):
    with pytest.raises(HTTPException) as exc_info:
        await ui_state_api.get_workspace_ui_state(
            workspace_id="does-not-exist", current_user={"id": "u"}
        )
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_get_swallows_corrupt_json(fake_workspace):
    (fake_workspace.workspace_dir / "ui_state.json").write_text(
        "{not-valid", encoding="utf-8"
    )
    result = await ui_state_api.get_workspace_ui_state(
        workspace_id="ws1", current_user={"id": "u"}
    )
    assert result == ui_state_api.WorkspaceUiState()


@pytest.mark.asyncio
async def test_get_swallows_non_object_json(fake_workspace):
    # Top-level array isn't an object — treat as empty rather than
    # crashing the analytics-tab boot flow that calls GET.
    (fake_workspace.workspace_dir / "ui_state.json").write_text(
        '["not", "an", "object"]', encoding="utf-8"
    )
    result = await ui_state_api.get_workspace_ui_state(
        workspace_id="ws1", current_user={"id": "u"}
    )
    assert result == ui_state_api.WorkspaceUiState()


@pytest.mark.asyncio
async def test_put_writes_file_and_echoes_payload(fake_workspace):
    payload = {"node_colors": {"node-a": "#2563eb", "node-b": "#dc2626"}}
    result = await ui_state_api.put_workspace_ui_state(
        workspace_id="ws1", payload=_state(payload), current_user={"id": "u"}
    )
    assert result.model_dump() == payload
    written = (fake_workspace.workspace_dir / "ui_state.json").read_text(
        encoding="utf-8"
    )
    assert json.loads(written) == payload


@pytest.mark.asyncio
async def test_put_replaces_existing_contents_not_merges(fake_workspace):
    (fake_workspace.workspace_dir / "ui_state.json").write_text(
        json.dumps({"node_colors": {"keep-me": "#000000"}}),
        encoding="utf-8",
    )
    new_payload = {"node_colors": {"new-only": "#2563eb"}}
    await ui_state_api.put_workspace_ui_state(
        workspace_id="ws1", payload=_state(new_payload), current_user={"id": "u"}
    )
    written = (fake_workspace.workspace_dir / "ui_state.json").read_text(
        encoding="utf-8"
    )
    # PUT is full replacement; the prior entry must be gone.
    assert json.loads(written) == new_payload


@pytest.mark.asyncio
async def test_put_404s_on_unknown_workspace(fake_workspace):
    with pytest.raises(HTTPException) as exc_info:
        await ui_state_api.put_workspace_ui_state(
            workspace_id="does-not-exist",
            payload=_state(),
            current_user={"id": "u"},
        )
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_put_creates_workspace_dir_if_missing(tmp_path, monkeypatch):
    # ``get_workspace_dir`` returns a path that doesn't exist yet —
    # PUT should create it (matches workspace_manager's general
    # "best-effort write" convention).
    target = tmp_path / "fresh"
    manager = _FakeManager(workspace_id="ws1", workspace_dir=target)
    monkeypatch.setattr(ui_state_api, "workspace_manager", manager)
    await ui_state_api.put_workspace_ui_state(
        workspace_id="ws1",
        payload=_state(),
        current_user={"id": "u"},
    )
    assert (target / "ui_state.json").exists()
