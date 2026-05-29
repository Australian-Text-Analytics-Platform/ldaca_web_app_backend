"""Join node endpoints: join two nodes, preview results.

Used by:
- Frontend and API clients through the FastAPI join routes.

Flow:
- Resolve workspace join endpoints, perform Polars joins,
- Return preview rows or persist a new joined child node.
"""

from __future__ import annotations

import math
from typing import Literal, cast

import polars as pl
from fastapi import APIRouter, Depends, HTTPException, Query

from ...core.auth import get_current_user
from ...models import FilterPreviewResponse, WorkspaceNodeInfo
from .schema_filter import frontend_node_info
from .utils import (
    _create_and_persist_child_node,
    require_current_workspace,
    update_workspace,
)

router = APIRouter(prefix="/workspaces", tags=["nodes"])


@router.post("/nodes/join/preview", response_model=FilterPreviewResponse)
async def join_nodes_preview(
    left_node_id: str,
    right_node_id: str,
    left_on: str | None = None,
    right_on: str | None = None,
    how: str = "inner",
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
):
    """Preview a join between two workspace nodes."""
    user_id = current_user["id"]
    workspace = require_current_workspace(user_id)
    left_node = workspace.nodes[left_node_id]
    right_node = workspace.nodes[right_node_id]
    try:
        allowed_hows = {"inner", "left", "right", "full", "semi", "anti", "cross"}
        how_val = (how or "inner").lower()
        if how_val not in allowed_hows:
            raise HTTPException(
                status_code=400,
                detail="Invalid join type. Allowed values: inner, left, right, full, semi, anti, cross",
            )
        join_how = cast(
            Literal["inner", "left", "right", "full", "semi", "anti", "cross"],
            how_val,
        )

        left_lazy = left_node.data
        right_lazy = right_node.data

        if join_how == "cross":
            joined_lazy = left_lazy.join(right_lazy, how="cross")
        else:
            if not left_on or not right_on:
                raise HTTPException(
                    status_code=400,
                    detail="left_on and right_on must be provided for non-cross joins",
                )
            joined_lazy = left_lazy.join(
                right_lazy, left_on=left_on, right_on=right_on, how=join_how
            )

        try:
            total_rows_df = cast(
                pl.DataFrame,
                joined_lazy.select(pl.len().alias("_len")).collect(),
            )
            total_rows_series = total_rows_df.to_series(0)
            total_rows = int(total_rows_series.item())
        except Exception:
            total_rows = None

        offset = (page - 1) * page_size
        try:
            preview_df = cast(
                pl.DataFrame, joined_lazy.slice(offset, page_size).collect()
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        preview_rows = preview_df.to_dicts()
        preview_columns = list(preview_df.columns)
        dtypes = {col: str(dtype) for col, dtype in preview_df.schema.items()}

        if total_rows is None:
            has_next = len(preview_rows) == page_size
            inferred_total = offset + len(preview_rows) + (page_size if has_next else 0)
            total_rows_value = inferred_total
            total_pages = max(1, page + (1 if has_next else 0))
        else:
            has_next = offset + page_size < total_rows
            total_rows_value = total_rows
            total_pages = max(1, math.ceil(total_rows / page_size))

        return {
            "data": preview_rows,
            "columns": preview_columns,
            "dtypes": dtypes,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_rows": total_rows_value,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_prev": page > 1,
            },
        }
    except KeyError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/nodes/join", response_model=WorkspaceNodeInfo)
async def join_nodes(
    left_node_id: str,
    right_node_id: str,
    left_on: str,
    right_on: str,
    how: str = "inner",
    new_node_name: str | None = None,
    current_user: dict = Depends(get_current_user),
):
    """Join two workspace nodes and persist the result as a new child node."""
    user_id = current_user["id"]
    workspace = require_current_workspace(user_id)
    workspace_id = workspace.id
    left_node = workspace.nodes[left_node_id]
    right_node = workspace.nodes[right_node_id]
    try:
        left_data = left_node.data
        right_data = right_node.data
        allowed_hows = {"inner", "left", "right", "full", "semi", "anti", "cross"}
        how_val = (how or "inner").lower()
        if how_val not in allowed_hows:
            raise HTTPException(
                status_code=400,
                detail="Invalid join type. Allowed values: inner, left, right, full, semi, anti, cross",
            )
        join_how = cast(
            Literal["inner", "left", "right", "full", "semi", "anti", "cross"],
            how_val,
        )
        if join_how == "cross":
            joined_data = left_data.join(right_data, how="cross")
        else:
            joined_data = left_data.join(
                right_data, left_on=left_on, right_on=right_on, how=join_how
            )
        node_name = new_node_name or f"{left_node.name}_join_{right_node.name}"
        new_node = _create_and_persist_child_node(
            workspace=workspace,
            data=joined_data,
            name=node_name,
            operation=f"join({left_node.name}, {right_node.name})",
            parents=[left_node, right_node],
            user_id=user_id,
            workspace_id=workspace_id,
        )
        return frontend_node_info(new_node)
    except KeyError:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
