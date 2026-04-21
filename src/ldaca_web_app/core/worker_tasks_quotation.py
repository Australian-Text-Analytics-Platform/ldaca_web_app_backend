"""Worker implementations for quotation background tasks."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, cast

from ..api.workspaces.analyses.generated_columns import (
    QUOTE_COLUMN_NAMES,
    QUOTE_QUOTE_COLUMN,
)

logger = logging.getLogger(__name__)


def _build_quotation_occurrence_dataframe(
    node_corpus: list[str],
    document_column: str,
    include_document_column: bool,
    extra_columns_data: Optional[Dict[str, list]],
):
    """Extract quotation occurrences from a corpus. Returns (df, output_columns)."""
    import polars as pl

    from ldaca_web_app.api.workspaces.analyses.quotation_core import (
        flatten_grouped_quotation_dataframe,
        quotation_groups_via_quote_extractor,
    )

    corpus = [str(v) if v is not None else "" for v in (node_corpus or [])]
    non_empty_mask = [bool(value.strip()) for value in corpus]
    filtered_corpus = [value for value, keep in zip(corpus, non_empty_mask) if keep]

    source_column_name = "__quotation_source__"
    data: dict[str, list] = {source_column_name: filtered_corpus}
    selected_columns: list[str] = []
    output_columns: list[str] = []

    if include_document_column:
        data[document_column] = filtered_corpus
        selected_columns.append(document_column)
        output_columns.append(document_column)

    if extra_columns_data:
        for col_name, col_values in extra_columns_data.items():
            filtered_values = [
                value for value, keep in zip(col_values, non_empty_mask) if keep
            ]
            data[col_name] = filtered_values
            selected_columns.append(col_name)
            output_columns.append(col_name)

    input_df = pl.DataFrame(data)
    quote_df = quotation_groups_via_quote_extractor(input_df, source_column_name)
    quote_df = flatten_grouped_quotation_dataframe(quote_df)
    generated_columns = [
        column_name
        for column_name in QUOTE_COLUMN_NAMES
        if column_name in quote_df.columns
    ]
    quote_df = quote_df.select(selected_columns + generated_columns)

    if QUOTE_QUOTE_COLUMN in quote_df.columns:
        quote_df = quote_df.filter(pl.col(QUOTE_QUOTE_COLUMN).is_not_null())

    return quote_df, output_columns + generated_columns


def run_quotation_detach_task(
    configure_worker_environment,
    workspace_dir: str,
    node_corpus: list[str],
    parent_node_id: str,
    document_column: str,
    engine_config: Dict[str, Any],
    new_node_name: str,
    include_document_column: bool = False,
    extra_columns_data: Optional[Dict[str, list]] = None,
    materialized_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """Run quotation detach and return a serialized detached node payload.

    Fast path: when `materialized_path` points to an existing parquet, wrap it
    directly as the detached node without re-extracting quotations.
    """
    configure_worker_environment()

    try:
        if progress_callback:
            progress_callback(0.02, "Loading quotation extractor...")

        import os

        import polars as pl

        from docworkspace import Node

        logger.info("[Worker %d] Starting quotation detach task", os.getpid())

        if materialized_path and os.path.exists(materialized_path):
            if progress_callback:
                progress_callback(0.4, "Reusing materialized quotations...")
            lazy = pl.scan_parquet(materialized_path)
            schema_names = list(lazy.collect_schema().names())
            record_count = int(
                cast(pl.DataFrame, lazy.select(pl.len()).collect()).item() or 0
            )
            detached_node = Node(
                data=lazy,
                name=new_node_name,
                workspace=None,
                operation="quotation_detach",
                parents=[parent_node_id],
                document=document_column,
            )
            node_payload = detached_node.to_dict(base_dir=workspace_dir)
            if progress_callback:
                progress_callback(1.0, "Quotation detach completed")
            return {
                "state": "successful",
                "result": {
                    "node_payload": node_payload,
                    "output_columns": schema_names,
                    "record_count": record_count,
                    "engine_config": engine_config,
                },
                "message": "Quotation detach completed successfully",
            }

        if progress_callback:
            progress_callback(0.2, "Preparing text data...")
        if progress_callback:
            progress_callback(0.6, "Extracting quotations...")

        quote_df, output_columns = _build_quotation_occurrence_dataframe(
            node_corpus=node_corpus,
            document_column=document_column,
            include_document_column=include_document_column,
            extra_columns_data=extra_columns_data,
        )

        if progress_callback:
            progress_callback(0.82, "Serializing detached data block...")

        detached_node = Node(
            data=quote_df.lazy(),
            name=new_node_name,
            workspace=None,
            operation="quotation_detach",
            parents=[parent_node_id],
            document=document_column,
        )
        node_payload = detached_node.to_dict(base_dir=workspace_dir)

        if progress_callback:
            progress_callback(1.0, "Quotation detach completed")

        logger.info(
            "[Worker %d] Quotation detach task completed successfully", os.getpid()
        )

        return {
            "state": "successful",
            "result": {
                "node_payload": node_payload,
                "output_columns": output_columns,
                "record_count": int(quote_df.height),
                "engine_config": engine_config,
            },
            "message": "Quotation detach completed successfully",
        }
    except Exception as e:
        return {
            "state": "failed",
            "error": str(e),
            "message": f"Quotation detach task failed: {str(e)}",
        }


def run_quotation_materialize_task(
    configure_worker_environment,
    workspace_dir: str,
    node_corpus: list[str],
    parent_task_id: str,
    parent_node_id: str,
    document_column: str,
    engine_config: Dict[str, Any],
    extra_columns_data: Optional[Dict[str, list]] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """Run full quotation extraction and persist the flattened parquet."""
    configure_worker_environment()

    try:
        import os

        if progress_callback:
            progress_callback(0.02, "Loading quotation extractor...")

        logger.info("[Worker %d] Starting quotation materialize task", os.getpid())

        if progress_callback:
            progress_callback(0.3, "Extracting quotations...")

        quote_df, output_columns = _build_quotation_occurrence_dataframe(
            node_corpus=node_corpus,
            document_column=document_column,
            include_document_column=True,
            extra_columns_data=extra_columns_data,
        )

        if progress_callback:
            progress_callback(0.85, "Writing materialized parquet...")

        materialized_dir = os.path.join(workspace_dir, "materialized", parent_task_id)
        os.makedirs(materialized_dir, exist_ok=True)
        materialized_path = os.path.join(materialized_dir, f"{parent_node_id}.parquet")
        quote_df.write_parquet(materialized_path)

        if progress_callback:
            progress_callback(1.0, "Quotation materialize completed")

        return {
            "state": "successful",
            "result": {
                "materialized_path": materialized_path,
                "parent_task_id": parent_task_id,
                "parent_node_id": parent_node_id,
                "output_columns": output_columns,
                "record_count": int(quote_df.height),
                "engine_config": engine_config,
            },
            "message": "Quotation materialize completed successfully",
        }
    except Exception as e:
        return {
            "state": "failed",
            "error": str(e),
            "message": f"Quotation materialize task failed: {str(e)}",
        }
