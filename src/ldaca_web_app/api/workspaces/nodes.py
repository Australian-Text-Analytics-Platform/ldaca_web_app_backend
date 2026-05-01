"""Node operation endpoints extracted from base.py.

Maintains identical routes and behavior to preserve backward compatibility.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import datetime
from typing import Any, Literal, Optional, cast

import polars as pl
from docworkspace.workspace.core import Workspace
from fastapi import APIRouter, Depends, HTTPException, Query

logger = logging.getLogger(__name__)

import json

from docworkspace import Node

from ...core.auth import get_current_user
from ...core.polars_expr_validator import (
    PolarsExprValidationError,
    ValidationResult,
    validate_polars_expr_code,
)
from ...core.polars_operations import get_operations_for_dtype
from ...core.utils import stringify_unsafe_integers
from ...core.workspace import workspace_manager
from ...models import (
    ConcatPreviewRequest,
    ConcatRequest,
    FilterPreviewResponse,
    FilterRequest,
    PaginationInfo,
    PolarsExpressionApplyResponse,
    PolarsExpressionContext,
    PolarsExpressionRequest,
    ReplaceApplyResponse,
    ReplaceRequest,
    SliceRequest,
)
from .utils import update_workspace

router = APIRouter(prefix="/workspaces", tags=["nodes"])


ISO_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+\-]\d{2}:?\d{2})$"
)


def _parse_temporal(value: Any) -> Any:
    """Parse ISO-like datetime strings into `datetime` objects when possible.

    Used by:
    - `_build_filter_expression`

    Why:
    - Enables temporal comparisons in filter operators.
    """
    if isinstance(value, str) and ISO_PATTERN.match(value):
        s = value
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if re.search(r"([+\-]\d{2})(\d{2})$", s):
            s = re.sub(r"([+\-]\d{2})(\d{2})$", r"\1:\2", s)
        try:
            return datetime.fromisoformat(s)
        except Exception:
            logger.debug("Could not parse temporal value: %s", value)
            return value
    return value


def _coerce_scalar(value: Any) -> Any:
    """Coerce string scalars into bool/int/float when safe.

    Used by:
    - `_build_filter_expression`

    Why:
    - Keeps query payload values aligned with Polars expression expectations.
    """
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in {"true", "false"}:
            return lowered == "true"
        try:
            if "." in value:
                return float(value)
            return int(value)
        except Exception:
            logger.debug("Could not coerce scalar: %s", value)
            return value
    return value


def _sanitize_column_alias(label: str) -> str:
    sanitized = re.sub(r"\s+", " ", label or "").strip()
    if not sanitized:
        return "computed_column"
    return sanitized[:120]


def _resolve_replace_column_name(request: ReplaceRequest) -> str:
    """Resolve final output column name for replace operations.

    Used by:
    - `replace_preview`
    - `replace_apply`

    Why:
    - Keeps overwrite-vs-new-column behavior consistent across both routes.
    """
    candidate = (request.output_column_name or request.source_column or "").strip()
    return _sanitize_column_alias(candidate)


def _is_string_list_dtype(dtype: Any) -> bool:
    """Return True when dtype is exactly a list of strings."""
    return dtype == pl.List(pl.String) or dtype == pl.List(pl.Utf8)


def _build_filter_expression(
    request: FilterRequest,
    column_dtypes: Optional[dict[str, Any]] = None,
) -> pl.Expr:
    logic = (request.logic or "and").lower()
    filter_expr = None
    schema_map = column_dtypes or {}

    for condition in request.conditions:
        column_expr = pl.col(condition.column)
        column_dtype = schema_map.get(condition.column)
        is_string_list_column = _is_string_list_dtype(column_dtype)
        op = condition.operator
        raw_value = condition.value
        expr = None

        if op in {
            "eq",
            "equals",
            "ne",
            "gt",
            "greater_than",
            "gte",
            "lt",
            "less_than",
            "lte",
        }:
            value = _coerce_scalar(_parse_temporal(raw_value))
            lit_val = pl.lit(value) if isinstance(value, datetime) else value
            if op in {"eq", "equals"}:
                expr = column_expr == lit_val
            elif op == "ne":
                expr = column_expr != lit_val
            elif op in {"gt", "greater_than"}:
                expr = column_expr > lit_val
            elif op == "gte":
                expr = column_expr >= lit_val
            elif op in {"lt", "less_than"}:
                expr = column_expr < lit_val
            elif op == "lte":
                expr = column_expr <= lit_val
        elif op == "in":
            include_null = False
            values: list[Any] = []

            if isinstance(raw_value, (list, tuple, set)):
                for item in raw_value:
                    if item is None:
                        include_null = True
                        continue
                    values.append(_coerce_scalar(_parse_temporal(item)))
            elif raw_value is None:
                include_null = True
            else:
                values = [_coerce_scalar(_parse_temporal(raw_value))]

            if is_string_list_column:
                string_values = [str(item) for item in values if item is not None]
                if string_values:
                    expr = column_expr.list.eval(
                        pl.element().cast(pl.String).is_in(string_values),
                        parallel=False,
                    ).list.any()
                else:
                    expr = pl.lit(False)
            else:
                if values:
                    expr = column_expr.is_in(values)
                    if include_null:
                        expr = expr | column_expr.is_null()
                elif include_null:
                    expr = column_expr.is_null()
        elif op == "contains":
            pattern = str(raw_value)
            case_sensitive = bool(getattr(condition, "case_sensitive", False))
            if getattr(condition, "regex", False):
                expr = column_expr.str.contains(pattern)
            elif case_sensitive:
                expr = column_expr.str.contains(pl.lit(pattern), literal=True)
            else:
                expr = column_expr.str.to_lowercase().str.contains(
                    pl.lit(pattern.lower()), literal=True
                )
        elif op == "startswith":
            expr = column_expr.str.starts_with(str(raw_value))
        elif op == "endswith":
            expr = column_expr.str.ends_with(str(raw_value))
        elif op == "is_null":
            expr = column_expr.is_null()
        elif op == "is_not_null":
            expr = column_expr.is_not_null()
        elif op == "between":
            expr = pl.lit(True)
            if isinstance(raw_value, dict):
                start_val = (
                    _parse_temporal(raw_value.get("start"))
                    if raw_value.get("start") is not None
                    else None
                )
                end_val = (
                    _parse_temporal(raw_value.get("end"))
                    if raw_value.get("end") is not None
                    else None
                )
                if start_val is not None and end_val is not None:
                    if isinstance(start_val, datetime):
                        start_val = pl.lit(start_val)
                    if isinstance(end_val, datetime):
                        end_val = pl.lit(end_val)
                    expr = column_expr.is_between(start_val, end_val, closed="both")
                elif start_val is not None:
                    if isinstance(start_val, datetime):
                        start_val = pl.lit(start_val)
                    expr = column_expr >= start_val
                elif end_val is not None:
                    if isinstance(end_val, datetime):
                        end_val = pl.lit(end_val)
                    expr = column_expr <= end_val
        else:
            expr = column_expr.str.contains(str(raw_value))

        if getattr(condition, "negate", False) and expr is not None:
            try:
                expr = expr.not_()
            except Exception:
                logger.debug("not_() failed for expression, falling back to ~ operator")
                expr = ~expr

        if expr is None:
            continue

        if filter_expr is None:
            filter_expr = expr
        else:
            filter_expr = (
                (filter_expr | expr) if logic == "or" else (filter_expr & expr)
            )

    if filter_expr is None:
        raise ValueError("No valid filter conditions provided")

    return filter_expr


def _require_current_workspace(user_id: str) -> Workspace:
    workspace = workspace_manager.get_current_workspace(user_id)
    if workspace is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return workspace


def _build_slice_or_sample_lazy(
    lazy_data: pl.LazyFrame,
    node_name: str,
    request: SliceRequest,
) -> tuple[pl.LazyFrame, str, str]:
    if request.mode == "random_sample":
        if request.sample_size is None:
            raise HTTPException(
                status_code=422,
                detail="sample_size is required when mode is 'random_sample'",
            )
        if request.sample_size < 1:
            sample_indices = pl.int_range(pl.len()).sample(
                fraction=request.sample_size,
                seed=request.random_seed,
            )
            sample_args = f"fraction={request.sample_size}"
        else:
            n = int(request.sample_size)
            sample_indices = pl.int_range(pl.len()).sample(
                n=n,
                seed=request.random_seed,
            )
            sample_args = f"n={n}"
        if request.random_seed is not None:
            sample_args = f"{sample_args}, seed={request.random_seed}"
        sampled_data = lazy_data.select(pl.all().gather(sample_indices))
        return (
            sampled_data,
            f"{node_name}_sampled",
            f"sample({node_name}, {sample_args})",
        )

    offset = int(request.offset or 0)
    length = request.length
    sliced_data = lazy_data.slice(offset, length)
    slice_args = f"offset={offset}"
    if length is not None:
        slice_args = f"{slice_args}, length={length}"
    return sliced_data, f"{node_name}_sliced", f"slice({node_name}, {slice_args})"


def _get_concat_nodes(user_id: str, node_ids: list[str]) -> list[Node]:
    if not node_ids:
        raise HTTPException(
            status_code=400, detail="At least two node IDs are required"
        )
    ws = _require_current_workspace(user_id)
    nodes: list[Node] = []
    seen: set[str] = set()
    for raw_node_id in node_ids:
        node_id = raw_node_id.strip()
        if not node_id:
            continue
        if node_id in seen:
            raise HTTPException(
                status_code=400,
                detail=f"Duplicate node id '{node_id}' provided",
            )
        node = ws.nodes[node_id]
        nodes.append(node)
        seen.add(node_id)
    if len(nodes) < 2:
        raise HTTPException(
            status_code=400,
            detail="At least two distinct nodes are required for concatenation",
        )
    return nodes


def _extract_lazy_schema(
    lazy_frame: pl.LazyFrame,
) -> tuple[list[str], dict[str, str]]:
    schema_dict = dict(lazy_frame.collect_schema().items())
    columns = list(schema_dict.keys())
    dtypes = {col: str(dtype) for col, dtype in schema_dict.items()}
    return columns, dtypes


def _build_replace_expression(request: ReplaceRequest) -> tuple[str, pl.Expr]:
    source_column = request.source_column.strip()
    column_name = _resolve_replace_column_name(request)

    col_expr = pl.col(source_column)
    connector = request.connector

    if request.mode == "extract":
        extracted = col_expr.str.extract_all(request.pattern)
        if request.count == "first":
            n = request.n if request.n is not None else 1
            extracted = extracted.list.head(n)
        joined = extracted.list.join(connector)
        expr = pl.when(extracted.list.len() > 0).then(joined).otherwise(pl.lit(None))
    else:
        if request.count == "all":
            expr = col_expr.str.replace_all(request.pattern, request.replacement)
        else:
            n = request.n if request.n is not None else 1
            expr = col_expr.str.replace(request.pattern, request.replacement, n=n)

    return column_name, expr.alias(column_name)


def _validate_and_align_concat_nodes(
    nodes: list[Node],
) -> tuple[list[pl.LazyFrame], list[str], dict[str, str]]:
    lazy_frames: list[pl.LazyFrame] = [node.data for node in nodes]
    base_columns, base_dtypes = _extract_lazy_schema(lazy_frames[0])
    if not base_columns:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine schema for the first node.",
        )

    select_expr = [pl.col(column) for column in base_columns]
    aligned_frames: list[pl.LazyFrame] = [lazy_frames[0].select(select_expr)]

    for node, lazy_frame in zip(nodes[1:], lazy_frames[1:]):
        columns, dtypes = _extract_lazy_schema(lazy_frame)
        missing = [col for col in base_columns if col not in columns]
        extra = [col for col in columns if col not in base_columns]
        mismatched = [
            col
            for col in base_columns
            if col in dtypes and base_dtypes.get(col) != dtypes.get(col)
        ]

        if missing or extra or mismatched:
            detail_parts: list[str] = []
            if missing:
                detail_parts.append("missing columns: " + ", ".join(sorted(missing)))
            if extra:
                detail_parts.append("unexpected columns: " + ", ".join(sorted(extra)))
            if mismatched:
                mismatch_details = ", ".join(
                    f"{col} ({base_dtypes.get(col)} vs {dtypes.get(col)})"
                    for col in sorted(mismatched)
                )
                detail_parts.append(f"type mismatches: {mismatch_details}")
            detail = (
                "Schema mismatch for node '"
                + node.name
                + "': "
                + "; ".join(detail_parts)
            )
            raise HTTPException(status_code=400, detail=detail)

        aligned_frames.append(lazy_frame.select(select_expr))

    return aligned_frames, base_columns, base_dtypes


def _calculate_concat_row_count(
    aligned_frames: list[pl.LazyFrame],
) -> Optional[int]:
    total = 0
    for lazy_frame in aligned_frames:
        try:
            count_df = cast(
                pl.DataFrame, lazy_frame.select(pl.len().alias("_len")).collect()
            )
            total += int(count_df.to_series(0).item())
        except Exception:
            logger.debug("Could not count rows for concat frame, returning None")
            return None
    return total


def _derive_concat_node_name(nodes: list[Node], desired_name: Optional[str]) -> str:
    if desired_name:
        return desired_name
    labels = [node.name for node in nodes]
    if not labels:
        return "Stack Result"
    if len(labels) <= 3:
        label_str = ", ".join(labels)
    else:
        label_str = ", ".join(labels[:3]) + ", ..."
    return f"Stack({label_str})"


@router.post(
    "/nodes/{node_id}/replace/preview",
    response_model=FilterPreviewResponse,
)
async def replace_preview(
    node_id: str,
    request: ReplaceRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    lazy_data = workspace.nodes[node_id].data

    try:
        _, replace_expr = _build_replace_expression(request)
        replaced_lazy = lazy_data.with_columns(replace_expr)

        total_rows_df = cast(
            pl.DataFrame,
            replaced_lazy.select(pl.len().alias("_len")).collect(),
        )
        total_rows = (
            int(total_rows_df.to_series(0).item())
            if total_rows_df.to_series(0).len()
            else 0
        )

        normalized_page_size = page_size
        total_pages = math.ceil(total_rows / normalized_page_size) if total_rows else 0
        normalized_page = min(max(page, 1), total_pages or 1)
        start_idx = (normalized_page - 1) * normalized_page_size if total_rows else 0

        preview_df = cast(
            pl.DataFrame,
            replaced_lazy.slice(start_idx, normalized_page_size).collect(),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return FilterPreviewResponse(
        data=preview_df.to_dicts(),
        columns=list(preview_df.columns),
        dtypes={col: str(dtype) for col, dtype in preview_df.schema.items()},
        pagination=PaginationInfo(
            page=normalized_page,
            page_size=normalized_page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            has_next=normalized_page < total_pages,
            has_prev=normalized_page > 1 and total_rows > 0,
        ),
    )


@router.post(
    "/nodes/{node_id}/replace",
    response_model=ReplaceApplyResponse,
)
async def replace_apply(
    node_id: str,
    request: ReplaceRequest,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]
    dtype_str: Optional[str] = None

    try:
        lazy_data = node.data
        column_name, replace_expr = _build_replace_expression(request)
        updated_data = lazy_data.with_columns(replace_expr)
        updated_schema = dict(updated_data.collect_schema().items())
        dtype = updated_schema.get(column_name)
        if dtype is not None:
            dtype_str = str(dtype)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        node.data = updated_data
        update_workspace(user_id, workspace_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return ReplaceApplyResponse(
        state="successful",
        node_id=node_id,
        column_name=column_name,
        dtype=dtype_str,
        message=f"Updated column '{column_name}' with regex replacement",
    )


@router.get("/nodes/{node_id}")
async def get_node_info(
    node_id: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    ws = _require_current_workspace(user_id)
    return ws.nodes[node_id].info()


@router.get("/nodes/{node_id}/query-plan")
async def get_node_query_plan(
    node_id: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    ws = _require_current_workspace(user_id)
    lazyframe = ws.nodes[node_id].data
    plan = lazyframe.explain(format="tree")
    return {"plan": plan}


@router.get("/nodes/{node_id}/data")
async def get_node_data(
    node_id: str,
    page: int = 1,
    page_size: int = 20,
    sort_by: Optional[str] = None,
    descending: bool = False,
    filter_column: Optional[str] = None,
    filter_value: Optional[str] = None,
    filter_op: str = "contains",
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    lf = _require_current_workspace(user_id).nodes[node_id].data
    schema = {col: str(dtype) for col, dtype in lf.collect_schema().items()}
    columns = list(schema.keys())

    # Apply column filter on the lazy frame before materialising.
    if filter_column and filter_value is not None and filter_column in columns:
        dtype_str = schema.get(filter_column, "")
        is_string_like = any(
            t in dtype_str.lower() for t in ("utf8", "string", "categorical")
        )
        if is_string_like:
            col_expr = pl.col(filter_column).cast(pl.Utf8)
            val = filter_value
            if filter_op == "eq":
                lf = lf.filter(col_expr == val)
            elif filter_op == "startswith":
                lf = lf.filter(col_expr.str.starts_with(val))
            elif filter_op == "endswith":
                lf = lf.filter(col_expr.str.ends_with(val))
            else:
                lf = lf.filter(col_expr.str.contains(val, literal=True))

    # Count total rows after filtering (cheap on LazyFrame).
    total_rows: int = cast(pl.DataFrame, lf.select(pl.len()).collect()).item()

    # Sort lazily before slicing.
    if sort_by and sort_by in columns:
        lf = lf.sort(sort_by, descending=descending, nulls_last=True)

    start_idx = (page - 1) * page_size
    page_df = cast(pl.DataFrame, lf.slice(start_idx, page_size).collect())

    return {
        "data": stringify_unsafe_integers(page_df.to_dicts()),
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_rows": total_rows,
            "total_pages": (total_rows + page_size - 1) // page_size,
            "has_next": start_idx + page_size < total_rows,
            "has_prev": page > 1,
        },
        "columns": columns,
        "dtypes": schema,
        "sorting": {
            "sort_by": sort_by if sort_by and sort_by in columns else None,
            "descending": descending,
        },
        "filtering": {
            "column": filter_column
            if filter_column and filter_column in columns
            else None,
            "value": filter_value,
            "op": filter_op,
        },
    }


@router.get("/nodes/{node_id}/shape")
async def get_node_shape(
    node_id: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    return {"shape": _require_current_workspace(user_id).nodes[node_id].shape}


@router.get("/nodes/{node_id}/columns/{column_name}/unique")
async def get_column_unique_values(
    node_id: str,
    column_name: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    try:
        lazyframe = _require_current_workspace(user_id).nodes[node_id].data
        schema = lazyframe.collect_schema()
        schema_map: dict[str, Any] = dict(schema.items())
        if _is_string_list_dtype(schema_map.get(column_name)):
            unique_df = cast(
                pl.DataFrame,
                lazyframe.select(pl.col(column_name).explode().alias(column_name))
                .unique(maintain_order=True)
                .collect(),
            )
            raw_values = unique_df.get_column(column_name).to_list()
            has_null = any(value is None for value in raw_values)
            deduped_values = [str(value) for value in raw_values if value is not None]

            return {
                "column_name": column_name,
                "unique_count": len(deduped_values) + (1 if has_null else 0),
                "unique_values": deduped_values,
                "has_null": has_null,
            }

        unique_df = cast(
            pl.DataFrame,
            lazyframe.select(pl.col(column_name).alias(column_name))
            .unique(maintain_order=True)
            .collect(),
        )
        raw_values = unique_df.get_column(column_name).to_list()
        has_null = any(value is None for value in raw_values)
        non_null_values = [value for value in raw_values if value is not None]
        return {
            "column_name": column_name,
            "unique_count": len(raw_values),
            "unique_values": non_null_values,
            "has_null": has_null,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/nodes/{node_id}/columns/{column_name}/describe")
async def describe_column(
    node_id: str,
    column_name: str,
    current_user: dict = Depends(get_current_user),
):
    """Get descriptive statistics for a column using Polars describe with 'nearest' interpolation."""
    from ...models import ColumnDescribeResponse

    user_id = current_user["id"]

    try:
        lazyframe = _require_current_workspace(user_id).nodes[node_id].data
        df = cast(pl.DataFrame, lazyframe.collect())

        column_dtype = df.schema[column_name]
        is_datetime_column = column_dtype in (
            pl.Datetime,
            pl.Datetime("ms"),
            pl.Datetime("us"),
            pl.Datetime("ns"),
        )

        desc_df = df.select(column_name).describe(interpolation="nearest")

        desc_dict = {}
        for row in desc_df.iter_rows(named=True):
            stat_name = row.get("statistic") or row.get("describe")
            if stat_name:
                desc_dict[stat_name] = row[column_name]

        def serialize_value(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val.isoformat()
            if is_datetime_column and isinstance(val, str) and val != "null":
                try:
                    dt = datetime.fromisoformat(val.replace(" ", "T"))
                    if dt.tzinfo is None:
                        from datetime import timezone

                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError, AttributeError:
                    return val
            try:
                return float(val)
            except TypeError, ValueError:
                return val

        return ColumnDescribeResponse(
            column_name=column_name,
            count=int(desc_dict.get("count", 0))
            if desc_dict.get("count") is not None
            else None,
            null_count=int(desc_dict.get("null_count", 0))
            if desc_dict.get("null_count") is not None
            else None,
            mean=serialize_value(desc_dict.get("mean")),
            std=serialize_value(desc_dict.get("std")),
            min=serialize_value(desc_dict.get("min")),
            percentile_25=serialize_value(desc_dict.get("25%")),
            median=serialize_value(desc_dict.get("50%")),
            percentile_75=serialize_value(desc_dict.get("75%")),
            max=serialize_value(desc_dict.get("max")),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/nodes/{node_id}")
async def delete_node(node_id: str, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    success = workspace.remove_node(node_id)
    if success:
        update_workspace(user_id, workspace_id)
    if not success:
        raise HTTPException(status_code=404, detail="Node not found")
    return {"state": "successful", "message": "Node deleted successfully"}


@router.put("/nodes/{node_id}/name")
async def update_node_name(
    node_id: str,
    new_name: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]
    node.name = new_name
    update_workspace(user_id, workspace_id, best_effort=True)
    try:
        return node.info()
    except Exception:
        logger.debug("node.info() failed for %s, returning minimal dict", node_id)
        return {"id": getattr(node, "id", node_id), "name": new_name}


@router.post("/nodes/{node_id}/clone")
async def clone_node(
    node_id: str,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]

    def _unique_clone_name(original: str) -> str:
        base = original or node_id
        candidate = f"{base}_clone"
        existing = {getattr(n, "name", None) for n in workspace.nodes.values()}
        if candidate not in existing:
            return candidate
        suffix = 2
        while f"{base}_clone_{suffix}" in existing:
            suffix += 1
        return f"{base}_clone_{suffix}"

    try:
        source_lazy = node.data
        cloned_lazy = source_lazy.clone()
        new_name = _unique_clone_name(getattr(node, "name", node_id))
        new_node = Node(
            data=cloned_lazy,
            name=new_name,
            workspace=workspace,
            operation=f"clone({getattr(node, 'name', node_id)})",
            parents=[node],
        )
        workspace.add_node(new_node)
        update_workspace(user_id, workspace_id)
        try:
            return new_node.info()
        except Exception:
            logger.debug("new_node.info() failed after clone, returning minimal dict")
            return {"id": getattr(new_node, "id", None), "name": new_name}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/nodes/{node_id}/filter")
async def filter_node(
    node_id: str,
    request: FilterRequest,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]
    lazy_data = node.data
    schema_map: dict[str, Any] = dict(lazy_data.collect_schema().items())
    filter_expr = _build_filter_expression(request, column_dtypes=schema_map)
    filtered_data = lazy_data.filter(filter_expr)
    new_node_name = request.new_node_name or f"{node.name}_filtered"
    new_node = Node(
        data=filtered_data,
        name=new_node_name,
        workspace=workspace,
        operation=f"filter({node.name})",
        parents=[node],
    )
    workspace.add_node(new_node)
    update_workspace(user_id, workspace_id)
    return {
        "node_name": new_node.name,
        "node_id": new_node.id,
    }


@router.post("/nodes/{node_id}/filter/preview")
async def filter_preview(
    node_id: str,
    request: FilterRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
) -> FilterPreviewResponse:
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    lazy_data = workspace.nodes[node_id].data

    try:
        schema_map: dict[str, Any] = dict(lazy_data.collect_schema().items())
        filter_expr = _build_filter_expression(request, column_dtypes=schema_map)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        filtered_lazy = lazy_data.filter(filter_expr)

        total_rows_df = cast(
            pl.DataFrame,
            filtered_lazy.select(pl.len().alias("_len")).collect(),
        )
        total_rows_series = total_rows_df.to_series(0)
        total_rows = int(total_rows_series.item()) if total_rows_series.len() else 0

        normalized_page_size = page_size
        total_pages = math.ceil(total_rows / normalized_page_size) if total_rows else 0
        normalized_page = min(max(page, 1), total_pages or 1)
        start_idx = (normalized_page - 1) * normalized_page_size if total_rows else 0

        preview_df = (
            cast(
                pl.DataFrame,
                filtered_lazy.slice(start_idx, normalized_page_size).collect(),
            )
            if total_rows
            else cast(
                pl.DataFrame,
                filtered_lazy.slice(0, normalized_page_size).collect(),
            )
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    columns = list(preview_df.columns)
    dtypes = {col: str(dtype) for col, dtype in preview_df.schema.items()}
    data_rows = preview_df.to_dicts()

    return FilterPreviewResponse(
        data=data_rows,
        columns=columns,
        dtypes=dtypes,
        pagination=PaginationInfo(
            page=normalized_page,
            page_size=normalized_page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            has_next=normalized_page < total_pages,
            has_prev=normalized_page > 1 and total_rows > 0,
        ),
    )


@router.post("/nodes/{node_id}/slice")
async def slice_node(
    node_id: str,
    request: SliceRequest,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]
    output_data, default_node_name, operation = _build_slice_or_sample_lazy(
        node.data,
        node.name,
        request,
    )
    new_node_name = request.new_node_name or default_node_name
    new_node = Node(
        data=output_data,
        name=new_node_name,
        workspace=workspace,
        operation=operation,
        parents=[node],
    )
    workspace.add_node(new_node)
    update_workspace(user_id, workspace_id)
    return {
        "node_name": new_node.name,
        "node_id": new_node.id,
    }


@router.post("/nodes/{node_id}/slice/preview")
async def slice_preview(
    node_id: str,
    request: SliceRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
) -> FilterPreviewResponse:
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)

    try:
        preview_lazy, _default_node_name, _operation = _build_slice_or_sample_lazy(
            workspace.nodes[node_id].data,
            workspace.nodes[node_id].name,
            request,
        )

        total_rows_df = cast(
            pl.DataFrame,
            preview_lazy.select(pl.len().alias("_len")).collect(),
        )
        total_rows_series = total_rows_df.to_series(0)
        total_rows = int(total_rows_series.item()) if total_rows_series.len() else 0

        normalized_page_size = page_size
        total_pages = math.ceil(total_rows / normalized_page_size) if total_rows else 0
        normalized_page = min(max(page, 1), total_pages or 1)
        preview_offset = (
            (normalized_page - 1) * normalized_page_size if total_rows else 0
        )

        preview_df = cast(
            pl.DataFrame,
            preview_lazy.slice(preview_offset, normalized_page_size).collect(),
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    columns = list(preview_df.columns)
    dtypes = {col: str(dtype) for col, dtype in preview_df.schema.items()}
    data_rows = preview_df.to_dicts()

    return FilterPreviewResponse(
        data=data_rows,
        columns=columns,
        dtypes=dtypes,
        pagination=PaginationInfo(
            page=normalized_page,
            page_size=normalized_page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            has_next=preview_offset + normalized_page_size < total_rows,
            has_prev=normalized_page > 1 and total_rows > 0,
        ),
    )


@router.post("/nodes/concat/preview")
async def concat_nodes_preview(
    request: ConcatPreviewRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    try:
        nodes = _get_concat_nodes(user_id, request.node_ids)
        aligned_frames, columns, dtypes = _validate_and_align_concat_nodes(nodes)
        concat_lazy = pl.concat(aligned_frames, how="vertical")
        total_rows = _calculate_concat_row_count(aligned_frames)

        normalized_page_size = page_size
        if total_rows is not None:
            total_pages = (
                math.ceil(total_rows / normalized_page_size) if total_rows else 0
            )
            normalized_page = min(max(page, 1), total_pages or 1)
            offset = (normalized_page - 1) * normalized_page_size if total_rows else 0
        else:
            total_pages = None
            normalized_page = max(page, 1)
            offset = (normalized_page - 1) * normalized_page_size

        preview_df = cast(
            pl.DataFrame,
            concat_lazy.slice(offset, normalized_page_size).collect(),
        )
        data_rows = preview_df.to_dicts()

        if total_rows is None:
            has_next = len(data_rows) == normalized_page_size
            inferred_total = (
                offset + len(data_rows) + (normalized_page_size if has_next else 0)
            )
            total_rows_value = inferred_total
            total_pages_value = max(1, normalized_page + (1 if has_next else 0))
        else:
            has_next = offset + normalized_page_size < total_rows
            total_rows_value = total_rows
            total_pages_value = total_pages

        pagination = {
            "page": normalized_page,
            "page_size": normalized_page_size,
            "total_rows": total_rows_value,
            "total_pages": total_pages_value,
            "has_next": has_next,
            "has_prev": normalized_page > 1 and (total_rows is None or total_rows > 0),
        }

        return {
            "data": data_rows,
            "columns": columns,
            "dtypes": dtypes,
            "pagination": pagination,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/nodes/concat")
async def concat_nodes(
    request: ConcatRequest,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    try:
        nodes = _get_concat_nodes(user_id, request.node_ids)
        aligned_frames, _, _ = _validate_and_align_concat_nodes(nodes)
        concat_lazy = pl.concat(aligned_frames, how="vertical")
        node_name = _derive_concat_node_name(nodes, request.new_node_name)
        labels = [node.name for node in nodes]
        parent_nodes: list[Node | str] = list(nodes)
        if len(labels) > 3:
            operation_args = ", ".join(labels[:3]) + ", ..."
        else:
            operation_args = ", ".join(labels)
        operation_label = f"concat({operation_args})"
        workspace = _require_current_workspace(user_id)
        workspace_id = workspace.id
        new_node = Node(
            data=concat_lazy,
            name=node_name,
            workspace=workspace,
            operation=operation_label,
            parents=parent_nodes,
        )
        workspace.add_node(new_node)
        update_workspace(user_id, workspace_id)
        return new_node.info()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/nodes/join/preview")
async def join_nodes_preview(
    left_node_id: str,
    right_node_id: str,
    left_on: Optional[str] = None,
    right_on: Optional[str] = None,
    how: str = "inner",
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
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
            logger.debug("Could not determine total rows for join preview")
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
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/nodes/join")
async def join_nodes(
    left_node_id: str,
    right_node_id: str,
    left_on: str,
    right_on: str,
    how: str = "inner",
    new_node_name: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
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
        new_node = Node(
            data=joined_data,
            name=node_name,
            workspace=workspace,
            operation=f"join({left_node.name}, {right_node.name})",
            parents=[left_node, right_node],
        )
        workspace.add_node(new_node)
        update_workspace(user_id, workspace_id)
        return new_node.info()
    except KeyError:
        raise
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# =============================================================================
# COLUMN OPERATIONS REGISTRY ENDPOINT
# =============================================================================


@router.get("/nodes/{node_id}/columns/{column_name}/operations")
async def column_operations(
    node_id: str,
    column_name: str,
    current_user: dict = Depends(get_current_user),
):
    """Return available no-arg Polars operations for a column, filtered by dtype."""
    user_id = current_user["id"]
    ws = _require_current_workspace(user_id)
    node = ws.nodes[node_id]
    schema = dict(node.data.collect_schema().items())
    dtype = schema.get(column_name)
    if dtype is None:
        raise HTTPException(status_code=404, detail=f"Column '{column_name}' not found")
    return {"operations": get_operations_for_dtype(dtype)}


# =============================================================================
# UNIFIED POLARS EXPRESSION ENDPOINTS
# =============================================================================


def _split_top_level_commas(code: str) -> list[str]:
    """Split *code* at commas that are not inside parentheses, brackets, braces, or strings."""
    segments: list[str] = []
    depth = 0
    current: list[str] = []
    in_string: str | None = None
    escape = False

    for char in code:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\":
            current.append(char)
            escape = True
            continue
        if in_string:
            current.append(char)
            if char == in_string:
                in_string = None
            continue
        if char in ('"', "'"):
            in_string = char
            current.append(char)
            continue
        if char in ("(", "[", "{"):
            depth += 1
        elif char in (")", "]", "}"):
            depth = max(0, depth - 1)
        if char == "," and depth == 0:
            segments.append("".join(current).strip())
            current = []
            continue
        current.append(char)

    remainder = "".join(current).strip()
    if remainder:
        segments.append(remainder)
    return segments


def _exec_polars_expr(code: str) -> list[pl.Expr]:
    """Execute polars expression code string(s) and return resulting pl.Expr(s).

    A single textarea may contain comma-separated segments.  Each segment is
    validated and executed individually, supporting:

    * Plain expression: ``pl.col("a")``
    * Assignment: ``name = pl.col("a")`` → ``pl.col("a").alias(name)``
    * Mixed: ``pl.col("a"), b = pl.col("x")``
    """
    segments = _split_top_level_commas(code.strip())
    if not segments:
        raise ValueError("Expression code cannot be empty")

    exprs: list[pl.Expr] = []
    for segment in segments:
        vr: ValidationResult = validate_polars_expr_code(segment)

        if vr.mode == "assign":
            expr_source = segment.split("=", 1)[1].strip()
        else:
            expr_source = segment

        ns: dict[str, object] = {"pl": pl}
        exec(f"_result = {expr_source}", {"__builtins__": {}}, ns)  # noqa: S102
        result = ns["_result"]

        if not isinstance(result, pl.Expr):
            raise ValueError(f"Expected pl.Expr, got {type(result).__name__}")

        if vr.mode == "assign" and vr.alias:
            result = result.alias(vr.alias)

        exprs.append(result)

    return exprs


def _apply_expression_context(
    lazy: pl.LazyFrame,
    request: PolarsExpressionRequest,
) -> pl.LazyFrame:
    """Apply the requested polars context + expressions to a LazyFrame."""
    context = request.context
    items = request.expressions

    if context == PolarsExpressionContext.filter:
        exprs = _exec_polars_expr(items[0].code)
        return lazy.filter(exprs[0])

    if context == PolarsExpressionContext.with_columns:
        exprs = [e for item in items for e in _exec_polars_expr(item.code)]
        return lazy.with_columns(exprs)

    if context == PolarsExpressionContext.select:
        exprs = [e for item in items for e in _exec_polars_expr(item.code)]
        return lazy.select(exprs)

    if context == PolarsExpressionContext.sort:
        pairs = [
            (e, bool(item.descending))
            for item in items
            for e in _exec_polars_expr(item.code)
        ]
        by = [p[0] for p in pairs]
        descending = [p[1] for p in pairs]
        return lazy.sort(by, descending=descending)

    if context == PolarsExpressionContext.group_by_agg:
        keys = [
            e for k in (request.group_by_keys or []) for e in _exec_polars_expr(k.code)
        ]
        aggs = [e for item in items for e in _exec_polars_expr(item.code)]
        return lazy.group_by(keys).agg(aggs)

    raise HTTPException(status_code=400, detail=f"Unknown context: {context}")


@router.post("/nodes/{node_id}/expression/preview")
async def polars_expression_preview(
    node_id: str,
    request: PolarsExpressionRequest,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=500),
    current_user: dict = Depends(get_current_user),
) -> FilterPreviewResponse:
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    lazy_data = workspace.nodes[node_id].data

    try:
        result_lazy = _apply_expression_context(lazy_data, request)
    except PolarsExprValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    total_rows_df = cast(
        pl.DataFrame,
        result_lazy.select(pl.len().alias("_len")).collect(),
    )
    total_rows = int(total_rows_df.to_series(0).item()) if total_rows_df.height else 0

    total_pages = math.ceil(total_rows / page_size) if total_rows else 0
    normalized_page = min(max(page, 1), total_pages or 1)
    start_idx = (normalized_page - 1) * page_size if total_rows else 0

    page_df = cast(
        pl.DataFrame,
        result_lazy.slice(start_idx, page_size).collect(),
    )
    columns, dtypes = _extract_lazy_schema(result_lazy)
    data = stringify_unsafe_integers(page_df.to_dicts())

    return FilterPreviewResponse(
        data=data,
        columns=columns,
        dtypes=dtypes,
        pagination=PaginationInfo(
            page=normalized_page,
            page_size=page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            has_next=normalized_page < total_pages,
            has_prev=normalized_page > 1,
        ),
    )


@router.post("/nodes/{node_id}/expression/apply")
async def polars_expression_apply(
    node_id: str,
    request: PolarsExpressionRequest,
    current_user: dict = Depends(get_current_user),
) -> PolarsExpressionApplyResponse:
    user_id = current_user["id"]
    workspace = _require_current_workspace(user_id)
    workspace_id = workspace.id
    node = workspace.nodes[node_id]

    try:
        result_lazy = _apply_expression_context(node.data, request)
    except PolarsExprValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # with_columns without an explicit new node name mutates the existing node
    # in place (adds/replaces columns), matching the behaviour of replace_apply.
    if request.context == PolarsExpressionContext.with_columns and not request.new_node_name:
        node.data = result_lazy
        update_workspace(user_id, workspace_id)
        return PolarsExpressionApplyResponse(node_id=node_id, node_name=node.name)

    new_node_name = request.new_node_name or f"{node.name}_{request.context}"
    new_node = Node(
        data=result_lazy,
        name=new_node_name,
        workspace=workspace,
        operation=f"expression({request.context}, {node.name})",
        parents=[node],
    )
    workspace.add_node(new_node)
    update_workspace(user_id, workspace_id)
    return PolarsExpressionApplyResponse(node_id=new_node.id, node_name=new_node.name)
