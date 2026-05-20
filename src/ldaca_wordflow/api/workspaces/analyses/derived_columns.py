"""Derived-column lifecycle endpoints (Phase 2.5 of multilingual).

POST  /workspaces/nodes/{node_id}/derived/tokens
    Tokenise a string column on the active workspace's node. Idempotent on
    ``(source_column, model)``: re-calling with the same pair replaces the
    existing derived column; a different model adds a second one.

POST  /workspaces/nodes/derived/tokens/bulk
    Re-tokenise every tokens-form derived column on each listed node, using
    the column's own previously-captured ``(source_column, model, language)``
    metadata. Powers the "Re-tokenise" button in the Workspace Graph
    title bar and the "Re-tokenise all" shortcut in the tokens-cache
    repair banner. Nodes without any tokens-form derived columns are
    silently skipped (returned in ``skipped`` rather than ``failed``).

DELETE /workspaces/nodes/{node_id}/derived/{column_name}
    Drop a previously-registered derived column from both the LazyFrame
    plan and ``Node.derived``. Returns 404 if the column wasn't registered
    on this node.

All endpoints persist the workspace via :func:`update_workspace` so the
derived metadata round-trips through plbin.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from pathlib import Path
from typing import List

from ....core.auth import get_current_user
from ....core.derived_columns import tokenise_column
from ....core.tokens_cache import (
    CacheReference,
    drop_reference as drop_cache_reference,
)
from ....core.workspace import workspace_manager
from ..utils import update_workspace
from .generated_columns import TOKENS_FORM

router = APIRouter(prefix="/workspaces", tags=["derived_columns"])
logger = logging.getLogger(__name__)


class TokeniseColumnRequest(BaseModel):
    """Body for ``POST /nodes/{node_id}/derived/tokens``."""

    source_column: str
    model: str
    language: Optional[str] = None


class TokeniseColumnResponse(BaseModel):
    """Result of a tokenise request — reports whether a new column was
    created or an existing one replaced.
    """

    column: str
    is_new: bool
    replaced_column: Optional[str] = None


@router.post(
    "/nodes/{node_id}/derived/tokens", response_model=TokeniseColumnResponse
)
async def create_derived_tokens(
    node_id: str,
    request: TokeniseColumnRequest,
    current_user: dict = Depends(get_current_user),
) -> TokeniseColumnResponse:
    user_id = current_user["id"]
    workspace = workspace_manager.get_current_workspace(user_id)
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if workspace is None or not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")
    if node_id not in workspace.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    node = workspace.nodes[node_id]

    # Snapshot whether a matching derived column already exists so we can
    # tell the caller whether their request created or replaced.
    existing = node.find_derived_column(
        request.source_column, form=TOKENS_FORM, model=request.model
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

    # Promote the just-tokenised column to the node's document so the
    # next analysis (and the next Tokenise dialog open) defaults to it
    # instead of resetting to the first column in the list. Mirrors the
    # same node.document promotion that concordance / token-frequency
    # already do at their entry points.
    try:
        node.document = request.source_column
    except Exception as exc:
        logger.debug(
            "Failed to set node.document after tokenise for node %s column %s: %s",
            node_id,
            request.source_column,
            exc,
        )

    update_workspace(user_id, workspace_id, best_effort=True)
    return TokeniseColumnResponse(
        column=derived_name,
        is_new=existing is None,
        replaced_column=existing,
    )


@router.delete("/nodes/{node_id}/derived/{column_name:path}")
async def delete_derived_column(
    node_id: str,
    column_name: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = workspace_manager.get_current_workspace(user_id)
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if workspace is None or not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")
    if node_id not in workspace.nodes:
        raise HTTPException(status_code=404, detail="Node not found")
    node = workspace.nodes[node_id]

    if column_name not in node.derived:
        raise HTTPException(
            status_code=404,
            detail=f"Derived column {column_name!r} not registered on this node",
        )

    # Capture the cache_filename before unregister so we can drop the
    # corresponding manifest reference. Older derived entries from before
    # the cache landed won't have this field — handle absence as "no
    # reference to drop" rather than an error.
    cache_filename: Optional[str] = (
        node.derived[column_name].get("cache_filename")
        if isinstance(node.derived[column_name], dict)
        else None
    )

    schema_names = node.data.collect_schema().names()
    if column_name in schema_names:
        node.data = node.data.drop(column_name, strict=False)
    node.unregister_derived_column(column_name)

    if cache_filename:
        drop_cache_reference(
            user_id,
            cache_filename,
            CacheReference(
                workspace_id=workspace_id,
                node_id=str(getattr(node, "id", node.name)),
            ),
        )

    update_workspace(user_id, workspace_id, best_effort=True)
    return {"state": "successful", "deleted_column": column_name}


__all__ = [
    "router",
    "BulkRetokeniseRequest",
    "BulkRetokeniseResponse",
    "BulkRetokeniseNodeResult",
    "TokeniseColumnRequest",
    "TokeniseColumnResponse",
]
