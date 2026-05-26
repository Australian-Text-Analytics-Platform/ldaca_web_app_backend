"""Derived-column lifecycle endpoints (Phase 2.5 of multilingual).

POST  /workspaces/nodes/{node_id}/derived/tokens
    Register the node's single tokenisation spec. A new source/model replaces
    any previous tokens spec for the node; token arrays are hydrated from the
    per-user DuckDB cache only inside analysis paths.

The endpoint persists the workspace via :func:`update_workspace` so the derived
metadata round-trips through plbin.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from docworkspace import Node

from ....core.auth import get_current_user
from ....core.derived_columns import tokenise_column
from ....core.workspace import workspace_manager
from ..utils import update_workspace
from .generated_columns import TOKENS_FORM

router = APIRouter(prefix="/workspaces", tags=["derived_columns"])
logger = logging.getLogger(__name__)


class TokeniseColumnRequest(BaseModel):
    """Body for ``POST /nodes/{node_id}/derived/tokens``."""

    source_column: str
    model: str
    language: str | None = None


class TokeniseColumnResponse(BaseModel):
    """Result of a tokenise request — reports whether a new column was
    created or an existing one replaced.
    """

    column: str
    is_new: bool
    replaced_column: str | None = None


def _get_active_workspace_node(user_id: str, node_id: str) -> tuple[str, Node]:
    workspace = workspace_manager.get_current_workspace(user_id)
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if workspace is None or not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")
    if node_id not in workspace.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    return workspace_id, workspace.nodes[node_id]


@router.post("/nodes/{node_id}/derived/tokens", response_model=TokeniseColumnResponse)
async def create_derived_tokens(
    node_id: str,
    request: TokeniseColumnRequest,
    current_user: dict = Depends(get_current_user),
) -> TokeniseColumnResponse:
    user_id = current_user["id"]
    workspace_id, node = _get_active_workspace_node(user_id, node_id)

    # A node carries at most one tokens spec. Snapshot any existing tokens entry
    # so the caller knows whether this request switched column/model.
    existing = next(
        (
            name
            for name, meta in node.derived.items()
            if isinstance(meta, dict) and meta.get("form") == TOKENS_FORM
        ),
        None,
    )

    try:
        derived_name = tokenise_column(
            node,
            source_column=request.source_column,
            model=request.model,
            language=request.language,
            user_id=user_id,
            workspace_id=workspace_id,
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    update_workspace(user_id, workspace_id, best_effort=True)
    return TokeniseColumnResponse(
        column=derived_name,
        is_new=existing is None,
        replaced_column=existing,
    )


__all__ = [
    "router",
    "TokeniseColumnRequest",
    "TokeniseColumnResponse",
]
