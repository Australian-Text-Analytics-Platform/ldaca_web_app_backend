"""Backwards-compatible re-export module for the split nodes sub-modules.

Previously a monolithic file (nodes.py), now split into:
  nodes_filter.py, nodes_slice.py, nodes_replace.py, nodes_concat.py,
  nodes_join.py, nodes_expression.py, nodes_crud.py.

This module re-exports all symbols so existing test imports from
``ldaca_wordflow.api.workspaces.nodes`` continue to work.
"""

from .nodes_concat import (
    _calculate_concat_row_count,
    _derive_concat_node_name,
    _get_concat_nodes,
    _validate_and_align_concat_nodes,
    concat_nodes,
    concat_nodes_preview,
)
from .nodes_crud import (
    clone_node,
    column_operations,
    delete_node,
    describe_column,
    get_column_unique_values,
    get_node_data,
    get_node_info,
    get_node_query_plan,
    get_node_shape,
    get_tokenizer_models,
    set_node_document_column,
    set_node_tokenization_preference,
    update_node_name,
)
from .nodes_expression import (
    _apply_expression_context,
    _exec_polars_expr,
    _split_top_level_commas,
    polars_expression_apply,
    polars_expression_preview,
)
from .nodes_filter import (
    _build_filter_expression,
    filter_node,
    filter_preview,
)
from .nodes_join import (
    join_nodes,
    join_nodes_preview,
)
from .nodes_replace import (
    _build_replace_expression,
    _resolve_replace_column_name,
    _sanitize_column_alias,
    replace_apply,
    replace_preview,
)
from .nodes_slice import (
    _build_slice_or_sample_lazy,
    slice_node,
    slice_preview,
)
from .utils import (
    _coerce_scalar,
    _create_and_persist_child_node,
    _extract_lazy_schema,
    _is_string_list_dtype,
    _make_temporal_literal,
    _paginated_lazy_preview,
    _parse_temporal,
    _propagated_tokenization,
    _serialize_column_scalar,
    require_current_workspace,
    require_current_workspace_id,
    update_workspace,
)
from ...core.workspace import workspace_manager
from docworkspace import Node  # re-exported for test monkey-patching
