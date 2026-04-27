"""Worker implementations for concordance background tasks."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, cast

from ..api.workspaces.analyses.concordance_core import build_concordance_search_pattern
from ..api.workspaces.analyses.generated_columns import (
    CONC_L1_COLUMN,
    CONC_L1_FREQ_COLUMN,
    CONC_MATCHED_TEXT_COLUMN,
    CONC_R1_COLUMN,
    CONC_R1_FREQ_COLUMN,
    CORE_CONCORDANCE_COLUMNS,
    MATERIALIZED_CONCORDANCE_COLUMNS,
    concordance_struct_projection,
)

logger = logging.getLogger(__name__)


def _build_concordance_occurrence_dataframe(
    node_corpus: list[str],
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    include_document_column: bool,
    extra_columns_data: Optional[Dict[str, list]],
):
    """Compute flattened occurrence rows for one corpus. Returns (df, output_columns)."""
    import polars as pl
    import polars_text as pt

    corpus = [str(v) if v is not None else "" for v in (node_corpus or [])]
    non_empty_mask = [bool(v.strip()) for v in corpus]
    corpus = [v for v, keep in zip(corpus, non_empty_mask) if keep]

    source_column_name = "__concordance_source__"
    data: dict[str, list] = {source_column_name: corpus}
    base_columns: list[pl.Expr] = []
    output_columns: list[str] = []

    if include_document_column:
        data[document_column] = corpus
        base_columns.append(pl.col(document_column))
        output_columns.append(document_column)

    if extra_columns_data:
        for col_name, col_values in extra_columns_data.items():
            filtered = [v for v, keep in zip(col_values, non_empty_mask) if keep]
            data[col_name] = filtered
            base_columns.append(pl.col(col_name))
            output_columns.append(col_name)

    df = pl.DataFrame(data)
    search_pattern, use_regex = build_concordance_search_pattern(
        search_word,
        regex=regex,
        whole_word=whole_word,
    )
    result = (
        df.select(
            [
                *base_columns,
                pt.concordance(
                    pl.col(source_column_name),
                    search_pattern,
                    num_left_tokens=num_left_tokens,
                    num_right_tokens=num_right_tokens,
                    regex=use_regex,
                    case_sensitive=case_sensitive,
                ).alias("concordance"),
            ]
        )
        .explode("concordance")
        .select(
            [
                pl.exclude("concordance"),
                *concordance_struct_projection("concordance"),
            ]
        )
        .filter(pl.col(CONC_MATCHED_TEXT_COLUMN).is_not_null())
    )
    return result, output_columns + list(CORE_CONCORDANCE_COLUMNS)


def run_concordance_detach_task(
    configure_worker_environment,
    workspace_dir: str,
    node_corpus: list[str],
    parent_node_id: str,
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    new_node_name: str,
    include_document_column: bool = False,
    extra_columns_data: Optional[Dict[str, list]] = None,
    materialized_path: Optional[str] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """Run concordance detach and return a serialized detached node payload.

    Fast path: when `materialized_path` points to an existing parquet (previously
    written by `run_concordance_materialize_task`), skip extraction and wrap the
    parquet as the detached node. Otherwise perform full concordance extraction.
    """
    configure_worker_environment()

    try:
        if progress_callback:
            progress_callback(0.02, "Loading concordance libraries...")

        import os

        import polars as pl

        from docworkspace import Node

        logger.info("[Worker %d] Starting concordance detach task", os.getpid())

        if materialized_path and os.path.exists(materialized_path):
            if progress_callback:
                progress_callback(0.4, "Reusing materialized occurrences...")
            import uuid

            # Read the materialized parquet and select only the columns the
            # user requested.  The materialized parquet contains all source
            # metadata columns so the detach fast path can respect the user's
            # column selection.
            mat_df = pl.read_parquet(materialized_path)

            # Determine which columns to keep.
            keep_cols: list[str] = []
            if include_document_column and document_column in mat_df.columns:
                keep_cols.append(document_column)
            if extra_columns_data:
                for col_name in extra_columns_data:
                    if col_name in mat_df.columns and col_name not in keep_cols:
                        keep_cols.append(col_name)
            # Always keep concordance-generated columns.
            for col in mat_df.columns:
                if col not in keep_cols:
                    if col.startswith("CONC_"):
                        keep_cols.append(col)
            mat_df = mat_df.select(keep_cols) if keep_cols else mat_df

            detach_data_dir = os.path.join(workspace_dir, "data")
            os.makedirs(detach_data_dir, exist_ok=True)
            detach_parquet_path = os.path.join(
                detach_data_dir,
                f"concordance_detach_{uuid.uuid4().hex}.parquet",
            )
            mat_df.write_parquet(detach_parquet_path)
            lazy = pl.scan_parquet(detach_parquet_path)
            schema_names = list(lazy.collect_schema().names())
            record_count = len(mat_df)
            output_columns = schema_names
            detached_node = Node(
                data=lazy,
                name=new_node_name,
                workspace=None,
                operation="concordance_detach",
                parents=[parent_node_id],
                document=document_column,
            )
            node_payload = detached_node.to_dict(base_dir=workspace_dir)
            if progress_callback:
                progress_callback(1.0, "Concordance detach completed")
            return {
                "state": "successful",
                "result": {
                    "node_payload": node_payload,
                    "output_columns": output_columns,
                    "record_count": record_count,
                },
                "message": "Concordance detach completed successfully",
            }

        if progress_callback:
            progress_callback(0.2, "Preparing text data...")

        if progress_callback:
            progress_callback(0.55, "Generating concordance matches...")

        result, output_columns = _build_concordance_occurrence_dataframe(
            node_corpus=node_corpus,
            document_column=document_column,
            search_word=search_word,
            num_left_tokens=num_left_tokens,
            num_right_tokens=num_right_tokens,
            regex=regex,
            whole_word=whole_word,
            case_sensitive=case_sensitive,
            include_document_column=include_document_column,
            extra_columns_data=extra_columns_data,
        )

        # Compute frequency columns (same as materialize) so detach always
        # includes CONC_l1_freq and CONC_r1_freq regardless of prior
        # materialization.
        l1_freq = (
            result.group_by(CONC_L1_COLUMN)
            .len()
            .rename({"len": CONC_L1_FREQ_COLUMN})
        )
        r1_freq = (
            result.group_by(CONC_R1_COLUMN)
            .len()
            .rename({"len": CONC_R1_FREQ_COLUMN})
        )
        result = result.join(l1_freq, on=CONC_L1_COLUMN, how="left").join(
            r1_freq, on=CONC_R1_COLUMN, how="left"
        )
        output_columns = output_columns + [CONC_L1_FREQ_COLUMN, CONC_R1_FREQ_COLUMN]

        if progress_callback:
            progress_callback(0.82, "Serializing detached data block...")

        detached_node = Node(
            data=result.lazy(),
            name=new_node_name,
            workspace=None,
            operation="concordance_detach",
            parents=[parent_node_id],
            document=document_column,
        )
        node_payload = detached_node.to_dict(base_dir=workspace_dir)

        if progress_callback:
            progress_callback(1.0, "Concordance detach completed")

        logger.info(
            "[Worker %d] Concordance detach task completed successfully", os.getpid()
        )

        return {
            "state": "successful",
            "result": {
                "node_payload": node_payload,
                "output_columns": output_columns,
                "record_count": len(result),
            },
            "message": "Concordance detach completed successfully",
        }
    except Exception as e:
        return {
            "state": "failed",
            "error": str(e),
            "message": f"Concordance detach task failed: {str(e)}",
        }


def run_concordance_materialize_task(
    configure_worker_environment,
    workspace_dir: str,
    node_corpus: list[str],
    parent_task_id: str,
    parent_node_id: str,
    document_column: str,
    search_word: str,
    num_left_tokens: int,
    num_right_tokens: int,
    regex: bool,
    whole_word: bool,
    case_sensitive: bool,
    extra_columns_data: Optional[Dict[str, list]] = None,
    progress_callback: Optional[Callable[[float, str], None]] = None,
) -> Dict[str, Any]:
    """Run full concordance extraction and persist the flattened parquet.

    Unlike detach, no Node is produced. The cached parquet is recorded on the
    parent analysis task so subsequent pagination and detach can reuse it.
    """
    configure_worker_environment()

    try:
        import os

        if progress_callback:
            progress_callback(0.02, "Loading concordance libraries...")

        logger.info("[Worker %d] Starting concordance materialize task", os.getpid())

        if progress_callback:
            progress_callback(0.25, "Generating concordance matches...")

        result, output_columns = _build_concordance_occurrence_dataframe(
            node_corpus=node_corpus,
            document_column=document_column,
            search_word=search_word,
            num_left_tokens=num_left_tokens,
            num_right_tokens=num_right_tokens,
            regex=regex,
            whole_word=whole_word,
            case_sensitive=case_sensitive,
            include_document_column=True,
            extra_columns_data=extra_columns_data,
        )

        import polars as pl

        l1_freq = (
            result.group_by(CONC_L1_COLUMN).len().rename({"len": CONC_L1_FREQ_COLUMN})
        )
        r1_freq = (
            result.group_by(CONC_R1_COLUMN).len().rename({"len": CONC_R1_FREQ_COLUMN})
        )
        result = result.join(l1_freq, on=CONC_L1_COLUMN, how="left").join(
            r1_freq, on=CONC_R1_COLUMN, how="left"
        )
        output_columns = output_columns + [CONC_L1_FREQ_COLUMN, CONC_R1_FREQ_COLUMN]

        if progress_callback:
            progress_callback(0.85, "Writing materialized parquet...")

        materialized_dir = os.path.join(workspace_dir, "data")
        os.makedirs(materialized_dir, exist_ok=True)
        materialized_path = os.path.join(
            materialized_dir,
            f".materialized_concordance_{parent_task_id}_{parent_node_id}.parquet",
        )
        result.write_parquet(materialized_path)

        if progress_callback:
            progress_callback(1.0, "Concordance materialize completed")

        return {
            "state": "successful",
            "result": {
                "materialized_path": materialized_path,
                "parent_task_id": parent_task_id,
                "parent_node_id": parent_node_id,
                "output_columns": output_columns,
                "record_count": int(len(result)),
            },
            "message": "Concordance materialize completed successfully",
        }
    except Exception as e:
        return {
            "state": "failed",
            "error": str(e),
            "message": f"Concordance materialize task failed: {str(e)}",
        }
