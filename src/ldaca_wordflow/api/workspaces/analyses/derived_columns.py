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
from ....core.tokens_cache_repair import clear_node_from_sidecar
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

    update_workspace(user_id, workspace_id, best_effort=True)
    # If this node was flagged as needing retokenise after a cross-machine
    # workspace import, clear the flag now that real tokens have been
    # written. The banner disappears the next time the frontend reads
    # /workspaces/info. See backend/docs/developer-guide/tokens-cache-portability.md.
    workspace_dir = getattr(workspace, "ws_root_dir", None)
    if isinstance(workspace_dir, Path):
        clear_node_from_sidecar(workspace_dir, node_id)
    return TokeniseColumnResponse(
        column=derived_name,
        is_new=existing is None,
        replaced_column=existing,
    )


class BulkRetokeniseRequest(BaseModel):
    """Body for ``POST /workspaces/nodes/derived/tokens/bulk``."""

    node_ids: List[str]


class BulkRetokeniseNodeResult(BaseModel):
    """Per-node outcome for a bulk re-tokenise call."""

    node_id: str
    rebuilt_columns: List[str] = []
    reason: Optional[str] = None
    error: Optional[str] = None


class BulkRetokeniseResponse(BaseModel):
    succeeded: List[BulkRetokeniseNodeResult] = []
    failed: List[BulkRetokeniseNodeResult] = []
    skipped: List[BulkRetokeniseNodeResult] = []


def _tokens_derived_entries(node) -> list[tuple[str, dict]]:
    """Return ``[(derived_name, metadata), ...]`` for every tokens-form
    derived column on ``node``. Skips entries whose metadata is missing the
    fields we need to re-derive (``source_column`` + ``model``)."""
    entries: list[tuple[str, dict]] = []
    derived = getattr(node, "derived", None) or {}
    for column_name, meta in derived.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("form") != TOKENS_FORM:
            continue
        if not meta.get("source_column") or not meta.get("model"):
            continue
        entries.append((column_name, meta))
    return entries


@router.post(
    "/nodes/derived/tokens/bulk", response_model=BulkRetokeniseResponse
)
async def bulk_retokenise_nodes(
    request: BulkRetokeniseRequest,
    current_user: dict = Depends(get_current_user),
) -> BulkRetokeniseResponse:
    """Re-tokenise every tokens-form derived column on each requested node.

    Powers two UI affordances: the banner's "Re-tokenise all" shortcut
    (which submits every node in the repair sidecar) and the title-bar
    "Re-tokenise" button (which submits the user's selection). Each node
    is processed independently; partial failures don't abort the batch.
    """
    user_id = current_user["id"]
    workspace = workspace_manager.get_current_workspace(user_id)
    workspace_id = workspace_manager.get_current_workspace_id(user_id)
    if workspace is None or not workspace_id:
        raise HTTPException(status_code=404, detail="No active workspace selected")

    workspace_dir = getattr(workspace, "ws_root_dir", None)
    response = BulkRetokeniseResponse()

    for node_id in request.node_ids:
        if node_id not in workspace.nodes:
            response.skipped.append(
                BulkRetokeniseNodeResult(
                    node_id=node_id, reason="node not found in workspace"
                )
            )
            continue
        node = workspace.nodes[node_id]
        entries = _tokens_derived_entries(node)
        if not entries:
            response.skipped.append(
                BulkRetokeniseNodeResult(
                    node_id=node_id,
                    reason="no tokens-form derived columns to rebuild",
                )
            )
            continue

        rebuilt: list[str] = []
        node_error: Optional[str] = None
        for _column_name, meta in entries:
            try:
                derived_name = tokenise_column(
                    node,
                    source_column=str(meta["source_column"]),
                    model=str(meta["model"]),
                    language=meta.get("language"),
                    user_id=user_id,
                    workspace_id=workspace_id,
                )
                rebuilt.append(derived_name)
            except Exception as exc:  # noqa: BLE001 - reported per-node
                logger.warning(
                    "bulk_retokenise: node %s column %s failed: %s",
                    node_id,
                    _column_name,
                    exc,
                )
                node_error = str(exc)
                break  # stop further columns on the same node — usually all share the same root cause

        if node_error is not None:
            response.failed.append(
                BulkRetokeniseNodeResult(
                    node_id=node_id, rebuilt_columns=rebuilt, error=node_error
                )
            )
            continue

        # Clear the sidecar entry — the node now has fresh tokens.
        if isinstance(workspace_dir, Path):
            clear_node_from_sidecar(workspace_dir, node_id)
        response.succeeded.append(
            BulkRetokeniseNodeResult(node_id=node_id, rebuilt_columns=rebuilt)
        )

    # Persist once at the end rather than once per node so we don't write
    # workspace.plbin N times in a row.
    if response.succeeded:
        update_workspace(user_id, workspace_id, best_effort=True)

    return response


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
