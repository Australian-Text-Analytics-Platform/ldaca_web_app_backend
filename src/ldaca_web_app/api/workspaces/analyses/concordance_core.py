"""Core concordance computation helpers shared by route handlers."""

from __future__ import annotations

import logging
import math
import re
from functools import partial
from typing import Any, Optional, cast

import polars as pl
from fastapi import HTTPException

from ....core.utils import stringify_unsafe_integers
from ....core.workspace import workspace_manager
from .generated_columns import (
    CONC_LEFT_CONTEXT_COLUMN,
    CONC_MATCHED_TEXT_COLUMN,
    CONC_RIGHT_CONTEXT_COLUMN,
    CORE_CONCORDANCE_COLUMNS,
    MATERIALIZED_CONCORDANCE_COLUMNS,
    concordance_struct_projection,
)
from .page_size_estimation import DEFAULT_PAGE_SIZE_CANDIDATES, estimate_page_size

logger = logging.getLogger(__name__)

DEFAULT_CONCORDANCE_PAGE = 1
DEFAULT_CONCORDANCE_PAGE_SIZE = 20
DEFAULT_CONCORDANCE_DESCENDING = True

_REQUEST_EXCLUDE_KEYS = {
    "page",
    "page_size",
    "sort_by",
    "descending",
    "pagination",
}


def normalize_saved_request(raw_request: Optional[dict]) -> Optional[dict]:
    """Normalize stored concordance request payloads.

    Used by:
    - `sanitize_request_for_storage`
    - concordance result endpoints before response rebuild

    Why:
    - Ensures persisted requests omit view-only keys and keep stable defaults.
    """
    if not raw_request:
        return None
    if "node_ids" not in raw_request or "node_columns" not in raw_request:
        return None

    normalized_request = dict(raw_request)
    if not normalized_request.get("combined"):
        normalized_request.pop("combined", None)
    for field in _REQUEST_EXCLUDE_KEYS:
        normalized_request.pop(field, None)

    normalized_request = {
        key: value for key, value in normalized_request.items() if value is not None
    }
    return normalized_request


def sanitize_request_for_storage(request_dict: dict[str, Any]) -> dict[str, Any]:
    """Return a storage-safe concordance request snapshot.

    Used by:
    - Concordance route handlers when persisting task requests.

    Why:
    - Prevents transient pagination/sorting fields from polluting saved inputs.
    """
    normalized = normalize_saved_request(request_dict)
    return normalized or {}


def concordance_non_empty_expr() -> pl.Expr:
    """Build an expression that removes empty concordance rows.

    Used by:
    - `build_concordance_lazyframe`

    Why:
    - Drops rows with no meaningful matched/context text before pagination.
    """
    return pl.any_horizontal(
        [
            pl.col(CONC_MATCHED_TEXT_COLUMN)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.len_chars()
            .fill_null(0)
            > 0,
            pl.col(CONC_LEFT_CONTEXT_COLUMN)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.len_chars()
            .fill_null(0)
            > 0,
            pl.col(CONC_RIGHT_CONTEXT_COLUMN)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.len_chars()
            .fill_null(0)
            > 0,
        ]
    )


def build_concordance_lazyframe(
    node_data: pl.LazyFrame,
    column: str,
    request: dict[str, Any],
) -> pl.LazyFrame:
    """Create concordance rows from a source LazyFrame and request options.

    Used by:
    - `compute_concordance_page`

    Why:
    - Encapsulates `polars_text.concordance` expansion and filtering in one
      reusable transformation step.
    """
    import polars_text as pt

    search_pattern, use_regex = build_concordance_search_pattern(
        request["search_word"],
        regex=bool(request["regex"]),
        whole_word=bool(request.get("whole_word", False)),
    )

    expr = pt.concordance(
        pl.col(column),
        search_pattern,
        num_left_tokens=request["num_left_tokens"],
        num_right_tokens=request["num_right_tokens"],
        regex=use_regex,
        case_sensitive=request["case_sensitive"],
    )
    return node_data.select([pl.all(), expr.alias("concordance")])


def build_concordance_search_pattern(
    search_word: str,
    *,
    regex: bool,
    whole_word: bool,
) -> tuple[str, bool]:
    """Return the effective concordance pattern and whether regex mode is needed."""
    if not whole_word:
        return search_word, regex

    base_pattern = search_word if regex else re.escape(search_word)
    return rf"\b(?:{base_pattern})\b", True


def _project_concordance_hit(raw_hit: dict[str, Any]) -> dict[str, Any]:
    """Project one raw concordance struct into canonical response columns."""
    return {
        CONC_LEFT_CONTEXT_COLUMN: raw_hit.get("left_context"),
        CONC_MATCHED_TEXT_COLUMN: raw_hit.get("matched_text"),
        CONC_RIGHT_CONTEXT_COLUMN: raw_hit.get("right_context"),
        "CONC_start_idx": raw_hit.get("start_idx"),
        "CONC_end_idx": raw_hit.get("end_idx"),
        "CONC_l1": raw_hit.get("l1"),
        "CONC_r1": raw_hit.get("r1"),
    }


def _concordance_hit_has_content(hit: dict[str, Any]) -> bool:
    """Return whether a projected concordance hit contains meaningful text."""
    for key in (
        CONC_MATCHED_TEXT_COLUMN,
        CONC_LEFT_CONTEXT_COLUMN,
        CONC_RIGHT_CONTEXT_COLUMN,
    ):
        value = hit.get(key)
        if value is None:
            continue
        if str(value).strip():
            return True
    return False


def _serialize_grouped_concordance_rows(
    result_df: pl.DataFrame,
    *,
    node_label: Optional[str] = None,
) -> tuple[list[list[dict[str, Any]]], list[str]]:
    """Serialize collected concordance rows into grouped per-document hit lists."""
    if result_df.height == 0:
        return [], []

    metadata_columns = [
        column for column in result_df.columns if column != "concordance"
    ]
    columns = [
        *metadata_columns,
        *CORE_CONCORDANCE_COLUMNS,
    ]
    if node_label:
        columns.append("__source_node")

    grouped_rows: list[list[dict[str, Any]]] = []
    for row in result_df.to_dicts():
        raw_hits = row.get("concordance") or []
        if not isinstance(raw_hits, list):
            continue

        base_row = {key: value for key, value in row.items() if key != "concordance"}
        grouped_hits: list[dict[str, Any]] = []
        for raw_hit in raw_hits:
            if not isinstance(raw_hit, dict):
                continue
            projected_hit = {
                **base_row,
                **_project_concordance_hit(raw_hit),
            }
            if node_label:
                projected_hit["__source_node"] = node_label
            if _concordance_hit_has_content(projected_hit):
                grouped_hits.append(projected_hit)

        if grouped_hits:
            grouped_rows.append(grouped_hits)

    return grouped_rows, columns


def resolve_node_sources(
    user_id: str,
    workspace_id: str,
    request: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, str]]:
    """Resolve workspace nodes into validated concordance source metadata.

    Used by:
    - `build_concordance_response`

    Why:
    - Centralizes node lookup, label mapping, and LazyFrame/type validation.
    """
    node_ids = request.get("node_ids") or []
    node_columns = request.get("node_columns") or {}

    node_sources: dict[str, dict[str, Any]] = {}
    label_to_node_map: dict[str, str] = {}
    node_labels: dict[str, str] = {}
    if workspace_manager.get_current_workspace_id(user_id) != workspace_id:
        if not workspace_manager.set_current_workspace(user_id, workspace_id):
            raise HTTPException(status_code=404, detail="Workspace not found")
    workspace = workspace_manager.get_current_workspace(user_id)
    if workspace is None:
        raise HTTPException(status_code=404, detail="Workspace not found")

    for node_id in node_ids:
        node = workspace.nodes[node_id]
        node_label = getattr(node, "name", None) or node_id
        label_to_node_map[node_label] = node_id
        node_labels[node_id] = node_label
        node_data = node.data
        column = node_columns.get(node_id)
        if not column:
            continue
        node_sources[node_id] = {
            "lf": node_data,
            "column": column,
            "label": node_label,
        }

    return node_sources, label_to_node_map, node_labels


def compute_concordance_page(
    base_lf: pl.LazyFrame,
    column: str,
    request: dict[str, Any],
    *,
    page: int,
    page_size: Optional[int],
    sort_by: Optional[str],
    descending: bool,
    node_label: Optional[str] = None,
) -> dict[str, Any]:
    """Compute one concordance page for a single node source.

    Used by:
    - `build_concordance_response`
    - `collect_interleaved_combined`

    Why:
    - Produces a stable page payload shape shared by single-node and combined
      result views. When `page_size` is None the size is estimated from the
      configured candidate ladder so the first page yields dense results.
    """
    total_rows_df = cast(pl.DataFrame, base_lf.select(pl.len()).collect())
    total_source_rows = total_rows_df.item()

    resolved_page_size = _resolve_page_size(base_lf, column, request, page_size)

    effective_sort_by: Optional[str] = None
    if sort_by:
        try:
            schema = base_lf.collect_schema()
            if sort_by in schema and sort_by not in CORE_CONCORDANCE_COLUMNS:
                base_lf = base_lf.sort(sort_by, descending=descending)
                effective_sort_by = sort_by
        except Exception as exc:
            logger.debug(
                "Ignoring unsupported sort_by '%s' for concordance page: %s",
                sort_by,
                exc,
            )

    start = max(page - 1, 0) * resolved_page_size
    page_lf = base_lf.slice(start, resolved_page_size)

    concordance_lf = build_concordance_lazyframe(page_lf, column, request)
    result_df = cast(pl.DataFrame, concordance_lf.collect())
    page_rows, columns = _serialize_grouped_concordance_rows(
        result_df,
        node_label=node_label,
    )

    total_source_pages = max(1, math.ceil(total_source_rows / resolved_page_size))

    metadata = {
        "concordance_columns": [c for c in columns if c in CORE_CONCORDANCE_COLUMNS],
        "metadata_columns": [c for c in columns if c not in CORE_CONCORDANCE_COLUMNS],
        "all_columns": columns,
    }

    return {
        "data": stringify_unsafe_integers(page_rows),
        "columns": columns,
        "metadata": metadata,
        "pagination": {
            "page": page,
            "page_size": resolved_page_size,
            "total_source_rows": total_source_rows,
            "total_source_pages": total_source_pages,
            "result_count": len(page_rows),
            "has_next": page < total_source_pages,
            "has_prev": page > 1,
        },
        "sorting": {
            "sort_by": effective_sort_by,
            "descending": descending,
        },
    }


def _count_concordance_hits(
    base_lf: pl.LazyFrame,
    column: str,
    request: dict[str, Any],
    size: int,
) -> int:
    """Return occurrence count when running concordance on the first `size` rows."""
    try:
        slice_lf = build_concordance_lazyframe(base_lf.slice(0, size), column, request)
        count_df = cast(
            pl.DataFrame,
            slice_lf.select(
                pl.col("concordance").list.len().fill_null(0).sum().alias("total")
            ).collect(),
        )
        value = count_df.item()
    except Exception as exc:
        logger.debug("Concordance hit probe failed at size=%d: %s", size, exc)
        return 0
    return int(value or 0)


def _resolve_page_size(
    base_lf: pl.LazyFrame,
    column: str,
    request: dict[str, Any],
    requested: Optional[int],
) -> int:
    """Return an effective page size, estimating when the client omitted one."""
    if requested is not None and int(requested) > 0:
        return int(requested)
    probe = partial(_count_concordance_hits, base_lf, column, request)
    return estimate_page_size(probe, candidates=DEFAULT_PAGE_SIZE_CANDIDATES)


def _serialize_materialized_rows(
    df: pl.DataFrame,
    *,
    node_label: Optional[str] = None,
) -> tuple[list[list[dict[str, Any]]], list[str]]:
    """Wrap each flat occurrence row as a single-hit group for UI compatibility."""
    if df.height == 0:
        return [], list(df.columns)

    columns = list(df.columns)
    grouped_rows: list[list[dict[str, Any]]] = []
    for row in df.to_dicts():
        hit = dict(row)
        if node_label:
            hit["__source_node"] = node_label
        grouped_rows.append([hit])

    if node_label and "__source_node" not in columns:
        columns.append("__source_node")
    return grouped_rows, columns


def compute_materialized_page(
    materialized_path: str,
    *,
    page: int,
    page_size: Optional[int],
    sort_by: Optional[str],
    descending: bool,
    node_label: Optional[str] = None,
) -> dict[str, Any]:
    """Paginate a materialized concordance parquet as occurrence rows."""
    effective_page_size = (
        int(page_size)
        if page_size is not None and int(page_size) > 0
        else DEFAULT_CONCORDANCE_PAGE_SIZE
    )
    lazy = pl.scan_parquet(materialized_path)
    total_rows = cast(pl.DataFrame, lazy.select(pl.len()).collect()).item() or 0

    effective_sort_by: Optional[str] = None
    if sort_by:
        try:
            schema = lazy.collect_schema()
            if sort_by in schema:
                lazy = lazy.sort(sort_by, descending=descending)
                effective_sort_by = sort_by
        except Exception as exc:
            logger.debug(
                "Ignoring unsupported sort_by '%s' for materialized page: %s",
                sort_by,
                exc,
            )

    start = max(page - 1, 0) * effective_page_size
    slice_df = cast(pl.DataFrame, lazy.slice(start, effective_page_size).collect())
    rows, columns = _serialize_materialized_rows(slice_df, node_label=node_label)

    total_source_pages = (
        max(1, math.ceil(total_rows / effective_page_size)) if total_rows else 0
    )

    metadata = {
        "concordance_columns": [
            c for c in columns if c in MATERIALIZED_CONCORDANCE_COLUMNS
        ],
        "metadata_columns": [
            c for c in columns if c not in MATERIALIZED_CONCORDANCE_COLUMNS
        ],
        "all_columns": columns,
    }

    return {
        "data": stringify_unsafe_integers(rows),
        "columns": columns,
        "metadata": metadata,
        "pagination": {
            "page": page,
            "page_size": effective_page_size,
            "total_source_rows": total_rows,
            "total_source_pages": total_source_pages,
            "result_count": len(rows),
            "has_next": page < total_source_pages,
            "has_prev": page > 1,
        },
        "sorting": {
            "sort_by": effective_sort_by,
            "descending": descending,
        },
        "materialized": True,
    }


def empty_concordance_page(page: int, page_size: int) -> dict[str, Any]:
    """Return an empty concordance page payload with metadata defaults.

    Used by:
    - `build_concordance_response` fallback paths

    Why:
    - Keeps response contracts consistent when no source rows are available.
    """
    return {
        "data": [],
        "columns": [],
        "metadata": {
            "concordance_columns": [],
            "metadata_columns": [],
            "all_columns": [],
        },
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_source_rows": 0,
            "total_source_pages": 0,
            "result_count": 0,
            "has_next": False,
            "has_prev": page > 1,
        },
        "sorting": {"sort_by": None, "descending": DEFAULT_CONCORDANCE_DESCENDING},
    }


def collect_interleaved_combined(
    left_base_lf: pl.LazyFrame,
    left_column: str,
    right_base_lf: pl.LazyFrame,
    right_column: str,
    request: dict[str, Any],
    *,
    page: int,
    page_size: Optional[int],
    sort_by: Optional[str],
    descending: bool,
    left_label: Optional[str] = None,
    right_label: Optional[str] = None,
) -> dict[str, Any]:
    """Combine two node concordance pages using left-right interleaving.

    Used by:
    - `build_concordance_response` when `combined=True` and two nodes are set.

    Why:
    - Preserves per-node page semantics while presenting a merged comparison view.
    """
    left_result = compute_concordance_page(
        left_base_lf,
        left_column,
        request,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        descending=descending,
        node_label=left_label,
    )
    right_result = compute_concordance_page(
        right_base_lf,
        right_column,
        request,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        descending=descending,
        node_label=right_label,
    )

    left_all_rows = left_result["data"]
    right_all_rows = right_result["data"]

    all_interleaved: list[dict[str, Any]] = []
    li, ri = 0, 0
    use_left = True
    while li < len(left_all_rows) or ri < len(right_all_rows):
        if use_left:
            if li < len(left_all_rows):
                all_interleaved.append(left_all_rows[li])
                li += 1
            elif ri < len(right_all_rows):
                all_interleaved.append(right_all_rows[ri])
                ri += 1
                use_left = not use_left
                continue
            else:
                break
        else:
            if ri < len(right_all_rows):
                all_interleaved.append(right_all_rows[ri])
                ri += 1
            elif li < len(left_all_rows):
                all_interleaved.append(left_all_rows[li])
                li += 1
                use_left = not use_left
                continue
            else:
                break
        use_left = not use_left

    columns = left_result.get("columns") or right_result.get("columns") or []
    if left_result.get("columns") and right_result.get("columns"):
        columns = list(dict.fromkeys(left_result["columns"] + right_result["columns"]))

    metadata = {
        "concordance_columns": [c for c in columns if c in CORE_CONCORDANCE_COLUMNS],
        "metadata_columns": [c for c in columns if c not in CORE_CONCORDANCE_COLUMNS],
        "all_columns": columns,
    }

    effective_sort_by = left_result["sorting"].get("sort_by") or right_result[
        "sorting"
    ].get("sort_by")

    left_pag = left_result["pagination"]
    right_pag = right_result["pagination"]
    total_source_rows = max(
        left_pag.get("total_source_rows", 0),
        right_pag.get("total_source_rows", 0),
    )
    total_source_pages = max(
        left_pag.get("total_source_pages", 0),
        right_pag.get("total_source_pages", 0),
    )
    resolved_page_size = (
        left_pag.get("page_size")
        or right_pag.get("page_size")
        or (int(page_size) if page_size is not None else DEFAULT_CONCORDANCE_PAGE_SIZE)
    )

    return {
        "data": all_interleaved,
        "columns": columns,
        "metadata": metadata,
        "pagination": {
            "page": page,
            "page_size": resolved_page_size,
            "total_source_rows": total_source_rows,
            "total_source_pages": total_source_pages,
            "result_count": len(all_interleaved),
            "has_next": page < total_source_pages,
            "has_prev": page > 1,
        },
        "sorting": {
            "sort_by": effective_sort_by,
            "descending": descending,
        },
    }


def build_concordance_response(
    user_id: str,
    workspace_id: str,
    request: dict[str, Any],
) -> dict[str, Any]:
    """Build the full concordance API response from a normalized request.

    Used by:
    - `concordance.run_concordance`
    - `concordance_task_result`
    - `concordance_task_result_post`

    Why:
    - Provides one shared response builder for run and retrieval endpoints,
      avoiding payload drift across routes.
    """
    page = int(request.get("page") or DEFAULT_CONCORDANCE_PAGE)
    raw_page_size = request.get("page_size")
    page_size: Optional[int] = (
        int(raw_page_size)
        if raw_page_size is not None and int(raw_page_size) > 0
        else None
    )
    sort_by = request.get("sort_by")
    descending = bool(request.get("descending", DEFAULT_CONCORDANCE_DESCENDING))
    combined = bool(request.get("combined"))
    materialized_paths_raw = request.get("materialized_paths") or {}
    materialized_paths: dict[str, str] = {
        str(node_id): str(path)
        for node_id, path in materialized_paths_raw.items()
        if isinstance(path, str) and path
    }

    node_ids = request.get("node_ids") or []

    node_sources, label_to_node_map, _node_labels = resolve_node_sources(
        user_id, workspace_id, request
    )
    data: dict[str, Any] = {}

    # Pre-resolve page_size for non-materialized nodes so combined/separated views
    # stay consistent. For materialized nodes we rely on the client's choice and
    # default to DEFAULT_CONCORDANCE_PAGE_SIZE inside compute_materialized_page.
    if page_size is None and combined is False:
        estimates: list[int] = []
        for node_id in node_ids:
            if node_id in materialized_paths:
                continue
            src = node_sources.get(node_id)
            if not src:
                continue
            estimates.append(
                _resolve_page_size(src["lf"], src["column"], request, None)
            )
        if estimates:
            page_size = max(estimates)
    if page_size is None and combined and len(node_ids) == 2:
        left_src = node_sources.get(node_ids[0])
        right_src = node_sources.get(node_ids[1])
        estimates_combined: list[int] = []
        if left_src and node_ids[0] not in materialized_paths:
            estimates_combined.append(
                _resolve_page_size(left_src["lf"], left_src["column"], request, None)
            )
        if right_src and node_ids[1] not in materialized_paths:
            estimates_combined.append(
                _resolve_page_size(right_src["lf"], right_src["column"], request, None)
            )
        if estimates_combined:
            page_size = max(estimates_combined)

    if combined and node_ids:
        if len(node_ids) == 2:
            left_id, right_id = node_ids
            left_src = node_sources.get(left_id)
            right_src = node_sources.get(right_id)
            if left_src and right_src:
                data["__COMBINED__"] = collect_interleaved_combined(
                    left_src["lf"],
                    left_src["column"],
                    right_src["lf"],
                    right_src["column"],
                    request,
                    page=page,
                    page_size=page_size,
                    sort_by=sort_by,
                    descending=descending,
                    left_label=left_src.get("label"),
                    right_label=right_src.get("label"),
                )
            else:
                data["__COMBINED__"] = empty_concordance_page(
                    page, page_size or DEFAULT_CONCORDANCE_PAGE_SIZE
                )
        else:
            all_rows: list[dict[str, Any]] = []
            columns: list[str] = []
            max_total_source_rows = 0
            max_total_source_pages = 0
            combined_page_size = page_size or DEFAULT_CONCORDANCE_PAGE_SIZE
            for node_id in node_ids:
                src = node_sources.get(node_id)
                if not src:
                    continue
                node_result = compute_concordance_page(
                    src["lf"],
                    src["column"],
                    request,
                    page=page,
                    page_size=combined_page_size,
                    sort_by=sort_by,
                    descending=descending,
                    node_label=src.get("label"),
                )
                all_rows.extend(node_result["data"])
                if not columns and node_result["columns"]:
                    columns = node_result["columns"]
                pag = node_result["pagination"]
                max_total_source_rows = max(
                    max_total_source_rows, pag.get("total_source_rows", 0)
                )
                max_total_source_pages = max(
                    max_total_source_pages, pag.get("total_source_pages", 0)
                )

            metadata = {
                "concordance_columns": [
                    c for c in columns if c in CORE_CONCORDANCE_COLUMNS
                ],
                "metadata_columns": [
                    c for c in columns if c not in CORE_CONCORDANCE_COLUMNS
                ],
                "all_columns": columns,
            }
            data["__COMBINED__"] = {
                "data": stringify_unsafe_integers(all_rows),
                "columns": columns,
                "metadata": metadata,
                "pagination": {
                    "page": page,
                    "page_size": combined_page_size,
                    "total_source_rows": max_total_source_rows,
                    "total_source_pages": max_total_source_pages,
                    "result_count": len(all_rows),
                    "has_next": page < max_total_source_pages,
                    "has_prev": page > 1,
                },
                "sorting": {"sort_by": sort_by, "descending": descending},
            }
        combinable = len(node_ids) > 1
    else:
        for node_id in node_ids:
            src = node_sources.get(node_id)
            if not src:
                continue
            if node_id in materialized_paths:
                data[node_id] = compute_materialized_page(
                    materialized_paths[node_id],
                    page=page,
                    page_size=page_size,
                    sort_by=sort_by,
                    descending=descending,
                    node_label=src.get("label"),
                )
                continue
            data[node_id] = compute_concordance_page(
                src["lf"],
                src["column"],
                request,
                page=page,
                page_size=page_size,
                sort_by=sort_by,
                descending=descending,
                node_label=src.get("label"),
            )
        combinable = len(node_ids) > 1

    analysis_params = dict(request)
    if label_to_node_map:
        analysis_params["label_to_node_map"] = label_to_node_map

    return {
        "state": "successful",
        "message": "Concordance analysis complete",
        "data": data,
        "analysis_params": analysis_params,
        "combinable": combinable,
    }
