"""Workspace UI-state sidecar endpoints.

Persists presentation-layer metadata into ``<workspace_dir>/ui_state.json`` — deliberately separate from docworkspace's
``metadata.json`` so the data-model serialisation stays free of UI concerns.

Endpoints:

    GET  /workspaces/{workspace_id}/ui-state
        Returns the parsed JSON, or ``{}`` when the file doesn't exist
        yet. 404 on unknown workspace.

    PUT  /workspaces/{workspace_id}/ui-state
        Replaces the file contents with the request body.

Why PUT (not PATCH): the payload is tiny (a flat object of < 50
entries in practice), the frontend already maintains the canonical
state in memory, and partial-merge semantics would mean we'd need a
recursive merger backend-side. Full replacement keeps both sides
simple.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from ...core.auth import get_current_user
from ...core.workspace import workspace_manager

router = APIRouter(prefix="/workspaces", tags=["workspace_ui_state"])
logger = logging.getLogger(__name__)

_UI_STATE_FILENAME = "ui_state.json"


class WorkspaceUiState(BaseModel):
    node_colors: dict[str, str] = Field(default_factory=dict)


def _ui_state_path_for(user_id: str, workspace_id: str) -> Path:
    workspace_dir = workspace_manager.get_workspace_dir(user_id, workspace_id)
    if workspace_dir is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return Path(workspace_dir) / _UI_STATE_FILENAME


@router.get("/{workspace_id}/ui-state")
async def get_workspace_ui_state(
    workspace_id: str,
    current_user: dict = Depends(get_current_user),
) -> WorkspaceUiState:
    user_id = current_user["id"]
    path = _ui_state_path_for(user_id, workspace_id)
    if not path.exists():
        return WorkspaceUiState()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "Failed to read ui_state.json for workspace %s: %s — returning default state",
            workspace_id,
            exc,
        )
        return WorkspaceUiState()
    if not isinstance(data, dict):
        logger.warning(
            "ui_state.json for workspace %s was not a JSON object — returning default state",
            workspace_id,
        )
        return WorkspaceUiState()
    return WorkspaceUiState.model_validate(data)


@router.put("/{workspace_id}/ui-state")
async def put_workspace_ui_state(
    workspace_id: str,
    payload: WorkspaceUiState,
    current_user: dict = Depends(get_current_user),
) -> WorkspaceUiState:
    user_id = current_user["id"]
    path = _ui_state_path_for(user_id, workspace_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload.model_dump(), f, ensure_ascii=False, indent=2)
    except OSError as exc:
        logger.error(
            "Failed to write ui_state.json for workspace %s: %s",
            workspace_id,
            exc,
        )
        raise HTTPException(
            status_code=500, detail="Failed to persist UI state"
        ) from exc
    return payload
