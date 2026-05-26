"""Tokenization lifecycle endpoints (Phase 2.5 of multilingual).

POST  /workspaces/nodes/{node_id}/tokenization
    Register one source column's tokenisation spec. A new model replaces that
    source column's previous spec; other source columns keep their specs. Token
    arrays are hydrated from the per-user DuckDB cache only inside analysis
    paths.

The endpoint persists the workspace via :func:`update_workspace` so the
tokenization metadata round-trips through plbin.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from docworkspace import Node

from ....core.auth import get_current_user
from ....core.tokenization import tokenise_column
from ....core.workspace import workspace_manager
from ..utils import update_workspace

router = APIRouter(prefix="/workspaces", tags=["tokenization"])
logger = logging.getLogger(__name__)


class TokeniseColumnRequest(BaseModel):
    """Body for ``POST /nodes/{node_id}/tokenization``."""

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


@router.post("/nodes/{node_id}/tokenization", response_model=TokeniseColumnResponse)
async def create_tokenization(
    node_id: str,
    request: TokeniseColumnRequest,
    current_user: dict = Depends(get_current_user),
) -> TokeniseColumnResponse:
    user_id = current_user["id"]
    workspace_id, node = _get_active_workspace_node(user_id, node_id)

    existing_meta = node.tokenization.get(request.source_column)
    existing = (
        existing_meta.get("column_name") if isinstance(existing_meta, dict) else None
    )

    try:
        tokenization_name = tokenise_column(
            node,
            source_column=request.source_column,
            model=request.model,
            language=request.language,
        )
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    update_workspace(user_id, workspace_id, best_effort=True)
    return TokeniseColumnResponse(
        column=tokenization_name,
        is_new=existing is None,
        replaced_column=existing,
    )


__all__ = [
    "router",
    "TokeniseColumnRequest",
    "TokeniseColumnResponse",
]
